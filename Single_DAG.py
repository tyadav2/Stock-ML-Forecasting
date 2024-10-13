#importing necessary libraries
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import logging

#setting up snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn

#task to extract data from alphavantage 
@task
def extract_stock_data():
    api_key = Variable.get("alpha_vantage_api_key")
    symbols = ["TTWO", "GOOGL"]
    stock_data = {}  # Empty dictionary

    for symbol in symbols:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
        response = requests.get(url)
        data = response.json()
        
        if "Time Series (Daily)" in data:
            # Limiting data to the latest 90 days
            daily_data = data["Time Series (Daily)"]
            latest_90_days_data = dict(list(daily_data.items())[:90])
            stock_data[symbol] = latest_90_days_data
        else:
            logging.error(f"No data for {symbol}: {data}")
    
    logging.info(stock_data)
    return stock_data  # Returning data limited to 90 days

#task to transform the extracted data
@task
def transform_stock_data(raw_data):
    transformed_data = [] #empty list

    for symbol, daily_data in raw_data.items():
        if not daily_data:
            logging.warning(f"No daily data for {symbol}.")
            continue

        #for loop to append the data in the desired format to load in snowflake
        for date, price_info in daily_data.items():
            transformed_data.append({
                'symbol': symbol,
                'date': date,
                'open': price_info['1. open'],
                'high': price_info['2. high'],
                'low': price_info['3. low'],
                'close': price_info['4. close'],
                'volume': price_info['5. volume']
            })
    
    logging.info(transformed_data)
    return transformed_data[:180]

#task to load transformed data into snowflake table
@task
def load_to_snowflake(data):
    conn = return_snowflake_conn() #opening connection to snowflake
    cur = conn.cursor() #defining cursor object
    try:
        #sql queries to create DB, Schema and tables
        cur.execute("CREATE DATABASE IF NOT EXISTS dev;")
        cur.execute("USE DATABASE dev;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_data;")
        cur.execute("USE SCHEMA raw_data;")        
        cur.execute("""
            CREATE OR REPLACE TABLE raw_data.stock_prices (
                symbol VARCHAR(10),
                date timestamp_ntz,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                PRIMARY KEY (symbol, date)
            );
        """)
        
        for record in data:
            try:
                sql = f"""
                    INSERT INTO dev.raw_data.stock_prices (symbol, date, open, high, low, close, volume)
                    VALUES ('{record['symbol']}', '{record['date']}', {record['open']}, {record['high']}, {record['low']}, {record['close']}, {record['volume']});
                """
                cur.execute(sql)
            except Exception as e:
                logging.error(f"Failed to insert record for {record['symbol']} on {record['date']}: {e}")

        cur.execute("COMMIT;") #on success loading commit changes
        
    except Exception as e:
        logging.error(f"Error occurred during loading to Snowflake: {e}")
        cur.execute("ROLLBACK;") #if failed for some reason, rollback to prev transcation before begin
        raise e
    finally:
        cur.close() #closing snowflake connection

#train task to train the ML model to predict stock prices
@task
def train_forecast_model(train_input_table, train_view, forecast_function_name):
    conn = return_snowflake_conn()
    cur = conn.cursor()
    
    # Creating view in schema adhoc
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS 
                          SELECT CAST(date AS TIMESTAMP_NTZ) AS DATE, 
                                 CLOSE, SYMBOL 
                          FROM {train_input_table};"""

    # Creating ML model in adhoc
    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""
    
    try:
        # Ensure that the adhoc schema exists
        cur.execute("USE DATABASE dev;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS adhoc;")  # Create adhoc schema if not exists
        cur.execute("USE SCHEMA dev.adhoc;")
        
        # Ensure that the analytics schema exists
        cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")  # Create analytics schema if not exists

        logging.info(f"Creating view with SQL: {create_view_sql}")
        cur.execute(create_view_sql)  # Create the view
        
        logging.info(f"Creating forecast model with SQL: {create_model_sql}")
        cur.execute(create_model_sql)  # Create the forecast model

        # Check evaluation metrics (optional)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    
    except Exception as e:
        logging.error(f"Error in train_forecast_model: {e}")
        raise
    finally:
        cur.close()
        conn.close()

#task to predict stock prices
@task
def predict_stock_prices(forecast_function_name, train_input_table, forecast_table, final_table):
    conn = return_snowflake_conn()
    cur = conn.cursor()
    
    # Ensure that the analytics schema exists
    cur.execute("USE SCHEMA dev.analytics;")
    
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""
    
    try:
        logging.info(f"Making predictions with SQL: {make_prediction_sql}")
        cur.execute(make_prediction_sql)
        
        logging.info(f"Creating final table with SQL: {create_final_table_sql}")
        cur.execute(create_final_table_sql)
    
    except Exception as e:
        logging.error(f"Error in predict_stock_prices: {e}")
        raise
    finally:
        cur.close()
        conn.close()
        
#dag information
with DAG(
    dag_id='stock_prediction_model_v1.1',
    start_date=datetime(2024, 10, 9),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock_prices', 'ETL', 'TTWO', 'GOOGL', 'ML', 'Forecast'] #tags to easily identify the dag in airflow
) as dag:

    raw_data = extract_stock_data()
    transformed_data = transform_stock_data(raw_data)
    load_task = load_to_snowflake(transformed_data)

    # Setting parameters for the next tasks
    train_input_table = "dev.raw_data.stock_prices"
    train_view = "dev.adhoc.stock_prices_view"
    forecast_function_name = "dev.analytics.predict_stock_price"
    forecast_table = "dev.adhoc.stock_prices_forecast"
    final_table = "dev.analytics.stock_prices_with_forecast"

    train_task = train_forecast_model(train_input_table, train_view, forecast_function_name)
    predict_task = predict_stock_prices(forecast_function_name, train_input_table, forecast_table, final_table)

    load_task >> train_task >> predict_task
