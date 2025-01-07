import boto3
import pandas as pd
import logging
from sklearn.preprocessing import OneHotEncoder

def data_ingestion(bucket_name):
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=bucket_name)

    for obj in objects['Contents']:
        s3_client.download_file(bucket_name, obj['Key'], f"data/{obj['Key']}")

def filter_data(date_column):
    df[date_column] = pd.to_datetime(df[date_column])
    df=df[df[date_column]<"2022-12-31"]
    df = df.set_index(date_column)
    return df

def create_time_series_features(df: pd.DataFrame):
    features = {
        'quarter': df.index.quarter,
        'day_of_week': df.index.dayofweek,
        'date_and_month': df.index.strftime('%m %b'),
        'month': df.index.month,
        'year': df.index.year,
        'day_of_month': df.index.day,
        'week_of_year': df.index.isocalendar().week
    }

    for feature_name, feature_values in features.items():
        df[feature_name] = feature_values

    return df

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

def split_data(df, date):  
    split_date = date

    train = df.loc[df.index < split_date]
    test = df.loc[df.index >= split_date]

    training_filtered_sales_data = train[['sku_id','warehouse_id','customer_type','day_of_month','day_of_week','month','quarter','year','week_of_year','order_quantity']]
    testing_filtered_sales_data = test[['sku_id','warehouse_id','customer_type','day_of_month','day_of_week','month','quarter','year','week_of_year','order_quantity']]  
    
    return training_filtered_sales_data, testing_filtered_sales_data

def data_processing():
    return
def model():
    return
def training():
    return
def prediction():
    return