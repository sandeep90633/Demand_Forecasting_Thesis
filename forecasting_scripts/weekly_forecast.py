from sqlalchemy import create_engine
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestRegressor
import argparse

# connecting to database to retrieve data
def conn(username, password, hostname, port, database_name):
    try:
        connection = create_engine(f"postgresql://{username}:{password}@{hostname}:{port}/{database_name}")
        
        print(f"Successfully connected to database: {database_name}")
        return connection
    except Exception as e:
        raise Exception (e)

# Retrieving data using query
def data_ingestion(conn, table_name):
    try:
        data = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        
        print(f"Read {table_name} data and exported it as a dataframe.")
        
        return data    
    except Exception as e:
        raise Exception (e)

# Extracting date features from the date column 
def create_time_series_features(col, df: pd.DataFrame):
    
    df[col] = pd.to_datetime(df[col], format='%d-%b-%y')
    df = df.set_index(col)
    
    features = {
        'month': df.index.month,
        'year': df.index.year,
        'week': df.index.isocalendar().week
    }

    for feature_name, feature_values in features.items():
        df[feature_name] = feature_values

    return df

# feature engineering for weekly sales data
def weekly_feature_engineering(df, cols, aggregate_col):
    df['lag_1'] = df.groupby(cols)[aggregate_col].shift(1)
    df['lag_2'] = df.groupby(cols)[aggregate_col].shift(2)
    df['lag_7'] = df.groupby(cols)[aggregate_col].shift(7)
    df['rolling_avg_3_weeks'] = df.groupby(cols)[aggregate_col].transform(lambda x: x.rolling(window=3).mean())
    df['cumulative_sum'] = df.groupby(cols)[aggregate_col].cumsum()

    # Removing events that have NaN values
    df = df.dropna(subset=['lag_1', 'lag_2','lag_7', 'rolling_avg_3_weeks', 'cumulative_sum'])
    
    return df

# feature engineering for monthly sales data
def monthly_feature_engineering(df, cols, aggregate_col):
    df['lag_1'] = df.groupby(cols)[aggregate_col].shift(1)
    df['lag_2'] = df.groupby(cols)[aggregate_col].shift(2)
    df['rolling_avg_3_months'] = df.groupby(cols)[aggregate_col].transform(lambda x: x.rolling(window=3).mean())
    df['cumulative_sum'] = df.groupby(cols)[aggregate_col].cumsum()

    # Removing events that have NaN values
    df = df.dropna(subset=['lag_1', 'lag_2', 'rolling_avg_3_months', 'cumulative_sum'])
    
    return df

# converting categorical columns into numeric columns
def oneHotEncoding(df, index_column):
    df=df.set_index(index_column)
    
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()

    encoder = OneHotEncoder(sparse_output=False)
    one_hot_encoded_categorical = encoder.fit_transform(df[categorical_cols])
    one_hot_df = pd.DataFrame(one_hot_encoded_categorical, columns=encoder.get_feature_names_out(categorical_cols))
    
    one_hot_encoded = pd.concat([df.reset_index(),one_hot_df], axis=1)
    one_hot_encoded = one_hot_encoded.drop(categorical_cols, axis=1)
    df = one_hot_encoded.set_index(index_column)

    return df

# Model definition
def model():
    random_forest_model = RandomForestRegressor(random_state=42, n_estimators=250, max_depth=9, min_samples_leaf=2, min_samples_split=6)
    
    print(f"Paramters of the model: {random_forest_model.get_params()}")
    
    return random_forest_model

def model_training(X, y):
    
    print("Features used in the model:", X.columns.tolist())
    
    rf_model = model()
    
    rf_model.fit(X, y)
    
    print('Model trained successfully...')
    
    return rf_model
    
def model_prediction(trained_model, data):
    
    predictions = trained_model.predict(data)
    
    return predictions
    
def main():
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-db_user', help='database username')
    parser.add_argument('-db_password', help='database password')
    parser.add_argument('-hostname', help='database host')
    parser.add_argument('-db_port',help='database port')
    parser.add_argument('-db_name',help='name of the database where data is stored')
    parser.add_argument('-training_data_table',help='training data table name which you want to download')
    parser.add_argument('-inventory_data_table',help='prediction data table name which you want to download')
    parser.add_argument('-period',help='Period to do prediction, include "week" or "month"')
    parser.add_argument('-year',help='For which year, forecasting should be made.')
    parser.add_argument('-month',help='For which month, forecasting should be made.')
    parser.add_argument('-week',help='For which week, forecasting should be made.')
    parser.add_argument('-predicted_data_file_path',help='prediction data table name which you want to download')
    
    args = parser.parse_args()
    
    if args.period is None or args.period!='week' or args.period!='month':
        print('Check period argument!')
        raise Exception
    
    if args.period == 'week' and (args.week == None or args.week == 0):
        print("Please include week number when you select 'week' period.")
        raise Exception
    
    connection = conn(args.db_user, args.db_password, args.hostname, args.db_port, args.db_name)
    
    sales = data_ingestion(connection, args.training_data_table)
    sales = create_time_series_features('order_date', sales)
    
    # As we dont have prediction data available, creating prediction data depending on the items present in inventory
    inventory_data = data_ingestion (connection, args.prediction_data_table)
    prediction_data = inventory_data[['sku_id', 'warehouse_id']]
    
    # Adding date features to prediction data. Can modify depending on which weeks or months we want to predict
    prediction_data['year'] = args.year
    prediction_data['month'] = args.month
    if args.period == 'week':
        prediction_data['week'] = args.week
    prediction_data['order_quantity'] = 0
    
    # Concatenating historical sales and prediction data, because to generate additional features for prediction data.
    sales = pd.concat([sales, prediction_data], ignore_index=True)
    
    # Depending on the period that user selected, aggregation will be done
    if args.period == 'week':
        sales = sales.groupby(['sku_id','warehouse_id','year','month', 'week'])['order_quantity'].sum().reset_index()
        sales = sales.sort_values(by=['year','month', 'week'])
        
        sales = weekly_feature_engineering(sales, ['sku_id','warehouse_id'], 'order_quantity')
    else:
        sales = sales.groupby(['sku_id','warehouse_id','year','month'])['order_quantity'].sum().reset_index()
        sales = sales.sort_values(by=['year','month'])
        
        sales = monthly_feature_engineering(sales, ['sku_id','warehouse_id'], 'order_quantity')
    
    sales = oneHotEncoding(sales, 'sku_id')
    
    training_data = sales.query("year<=2023 and week<31")
    prediction_data = sales.query("year==2023 and week==31")
    
    X_train = training_data.drop(columns=['order_quantity'])
    y_train = training_data['order_quantity']
    
    prediction_data = prediction_data.drop(columns=['order_quantity'])
    
    trained_model = model_training(X_train, y_train)
    
    predictions = model_prediction(trained_model, trained_model)
    
    predictions = pd.DataFrame(predictions, index=prediction_data.index, columns=['predicted_quantity'])
    
    predictions.to_csv(args.predicted_data_file_path)

if __name__ == "__main__":
    main()
    
