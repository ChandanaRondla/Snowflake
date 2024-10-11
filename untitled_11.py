#!/usr/bin/env python
# coding: utf-8

# In[1]:


from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import snowflake.connector
from airflow.models import Variable

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Define the DAG
with DAG(
    'colab_to_composer_example',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task to get Alpha Vantage API data
    @task
    def fetch_data():
        vantage_api = Variable.get('ALPHA_API_KEY')
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={vantage_api}'
        response = requests.get(url)
        data = response.json()
        return data

    # Task to process data and prepare results for insertion
    @task
    def process_data(data):
        results = []
        for d in data["Time Series (Daily)"]:
            stock_info = data["Time Series (Daily)"][d]
            stock_info["date"] = d
            results.append(stock_info)
        return results

    # Task to create a table in Snowflake
    @task
    def create_table():
        conn = snowflake.connector.connect(
            user=Variable.get('snowflake_userid'),
            password=Variable.get('snowflake_password'),
            account=Variable.get('snowflake_account')
        )
        create_table_query = """
        CREATE OR REPLACE TABLE stock_api (
          open DOUBLE,
          high DOUBLE,
          low DOUBLE,
          close DOUBLE,
          volume INT,
          date VARCHAR(512) PRIMARY KEY,
          symbol VARCHAR(20)
        );
        """
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS vantage_db;")
        cursor.execute("USE DATABASE vantage_db;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS api_schema;")
        cursor.execute("USE SCHEMA api_schema;")
        cursor.execute(create_table_query)
        cursor.close()
        conn.close()

    # Task to insert data into Snowflake
    @task
    def insert_data(results):
        conn = snowflake.connector.connect(
            user=Variable.get('snowflake_userid'),
            password=Variable.get('snowflake_password'),
            account=Variable.get('snowflake_account')
        )
        try:
            count = 0
            for r in results:
                open = float(r['1. open'])
                high = float(r['2. high'])
                low = float(r['3. low'])
                close = float(r['4. close'])
                volume = int(r['5. volume'])
                date = r['date']
                symbol = "AAPL"
                insert_sql = f"""
                    INSERT INTO stock_api (open, high, low, close, volume, date, symbol)
                    VALUES ({open}, {high}, {low}, {close}, {volume}, '{date}', '{symbol}')
                """
                conn.cursor().execute(insert_sql)
                count += 1
            conn.cursor().execute("COMMIT")
            print(f"Data inserted successfully, total rows: {count}")
        except Exception as e:
            conn.cursor().execute("ROLLBACK")
            print(f"An error occurred: {e}")
        finally:
            conn.close()

    # Define task dependencies
    data = fetch_data()
    processed_data = process_data(data)
    create_tbl = create_table()
    insert = insert_data(processed_data)

    # Set task order
    create_tbl >> data >> processed_data >> insert

