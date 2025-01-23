CREATE TABLE sales (
    order_number VARCHAR,
    order_date DATE,
    sku_id VARCHAR,
    warehouse_id VARCHAR,
    customer_type VARCHAR,
    order_quantity REAL,
    unit_sale_price REAL,
    revenue REAL
)

CREATE TABLE inventory (
    sku_id VARCHAR,
    vendor_name VARCHAR,
    warehouse_id VARCHAR,
    current_inventory_quantity REAL,
    cost_per_sku REAL,
    total_value REAL,
    units VARCHAR,
    average_lead_time REAL,
    maximum_lead_time REAL,
    unit_price REAL
)

CREATE TABLE weekly_prediction_data (
    sku_id VARCHAR,
    warehouse_id VARCHAR,
    year INTEGER,
    month INTEGER,
    week INTEGER,
    lag_1 REAL,
    lag_2 REAL,
    lag_7 REAL,
    rolling_avg_3_weeks REAL,
    cumulative_sum REAL
)

CREATE TABLE weekly_predicted_data (
    sku_id VARCHAR,
    predicted_quantity REAL
)

CREATE TABLE abc_xyz_table (
    sku_id TEXT,
    abc_category TEXT,
    xyz_category TEXT
)

CREATE TABLE actual_predict_quantity (    
    sku_id VARCHAR,
    year INTEGER,
    month INTEGER,
    week INTEGER,
    order_quantity REAL,
    predicted_quantity REAL
)

CREATE TABLE model_accuracy_metrics (
    RMSE REAL,
    R2_Score REAL
)