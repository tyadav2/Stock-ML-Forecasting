from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


#for initiating snowflake connection
def return_snowflake_conn():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn') # Initialize the SnowflakeHook

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

#extracting stock data
@task
def extract(vantage_api_key, symbols):
    data = {}
    for symbol in symbols:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
        response = requests.get(url)
        stock_data = response.json().get("Time Series (Daily)", {})

        #Sorting dates in descending order and taking the first 90 entries
        last_90_days = dict(list(stock_data.items())[:90])
        
        data[symbol] = last_90_days #storing only the last 90 days for each stock symbol
    return data

#transforming stock data
@task
def transform(data, symbols):
    results = []
    for symbol in symbols:
        for date, stock_info in data[symbol].items():
            stock_info["date"] = date
            stock_info["symbol"] = symbol  # Adding the symbol dynamically
            results.append(stock_info)
    return results

#loading the stock data in Snowflake
@task
def load_to_snowflake(records, target_table):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE OR REPLACE TABLE {target_table} (
                date timestamp_ntz primary key,
                open float,
                high float,
                low float,
                close float,
                volume bigint,
                symbol varchar
            );
        """)
        for r in records:
            date = r['date']
            open_price = r['1. open']
            high_price = r['2. high']
            low_price = r['3. low']
            close_price = r['4. close']
            volume = r['5. volume']
            symbol = r['symbol']

            sql = f"""
                INSERT INTO {target_table} (date, open, high, low, close, volume, symbol)
                VALUES (TO_TIMESTAMP_NTZ('{date}', 'YYYY-MM-DD'), {open_price}, {high_price}, {low_price}, {close_price}, {volume}, '{symbol}')
            """
            cur.execute(sql)

        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

    finally:
        cur.close()

with DAG(
    dag_id = 'Stock_price',
    start_date = datetime(2024,10,5),
    catchup=False,
    tags=['ETL', 'Lab1'],
    schedule = '@daily'
) as dag:

    target_table = 'dev.raw_data.stock_price'
    vantage_api_key = Variable.get('vantage_api_key')
    symbols = ['GOOGL', 'TTWO']

    #tasks
    data = extract(vantage_api_key, symbols)
    transformed_data = transform(data, symbols)
    load_to_snowflake(transformed_data, target_table)
