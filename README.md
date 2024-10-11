**#Project Overview**
This project demonstrates how to port a Python script from Google Colab to Cloud Composer (Airflow). The DAG fetches stock data from the Alpha Vantage API, processes it, and inserts it into a Snowflake table.

**#Requirements**
Cloud Composer (Airflow)
Snowflake Account
Alpha Vantage API Key

**#Setup**
Clone the Repository:
''' bash
      git clone https://github.com/yourusername/colab_to_composer_example.git
cd colab_to_composer_example

**#Install Dependencies:**
Ensure the following packages are installed in Cloud Composer:
1.snowflake-connector-python
2.pandas
3.requests

**#Set Airflow Variables:**
In Airflow, set the following:
1.ALPHA_API_KEY
2.snowflake_userid
3.snowflake_password
4.snowflake_account

**#DAG Overview**
1.Fetch Data: Pull stock data from Alpha Vantage.
2.Process Data: Prepare the data for insertion.
3.Create Table: Create the stock_api table in Snowflake.
4.Insert Data: Insert the data into Snowflake.

**#Usage**
1.Upload the DAG to your Composer environment's /dags folder.
2.Trigger the DAG manually or let it run on the daily schedule (@daily).
3.Verify data insertion in Snowflake.
