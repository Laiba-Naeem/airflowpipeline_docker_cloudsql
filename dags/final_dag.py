from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
import os
import logging
from dotenv import load_dotenv


"""This dag reads dataset and send the data to google cloud sql using docker and airflow pipeline"""

# Load the environment variables from .env file
load_dotenv()

# Retrieve the connection parameters from environment variables
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# # Create the connection string
db_connection_string = (
    f"postgresql+psycopg2://{db_username}:{db_password}@cloudsql:{db_port}/{db_name}"
)

# Define the DAG
dag = DAG(
    "final_dag",
    description="DAG to read CSV data and insert into the Hotel table",
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
)

engine = create_engine(db_connection_string)

def check_table():
    """This task creates the database connection with DB"""
    try:
        # Create a SQLAlchemy engine with the connection string
        engine = create_engine(db_connection_string)

        create_table_query = """
            CREATE TABLE IF NOT EXISTS hotel (
                hotel_id SERIAL PRIMARY KEY,
                hotel_name VARCHAR(255),
                hotel_location VARCHAR(255),
                bed_rooms INTEGER,
                latitude FLOAT,
                longitude FLOAT
            )
        """

        engine.execute(create_table_query)
    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle the exception or perform any necessary cleanup



def data_processing():
    """Following function reads the data from dataset"""
    try:
        # Get the absolute path of the DAGs directory
        dags_directory = os.path.abspath(os.path.dirname(__file__))

        # Construct the absolute path to the CSV file
        csv_file_path = os.path.join(dags_directory, "hotel_dataset.csv")

        # Read the CSV file
        df = pd.read_csv(csv_file_path)

        # Data pre-processing steps
        df = df.dropna(axis=0, subset=["id"])
        print(df.isnull().sum())
        print(df.head())

        # Return the DataFrame
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle the exception or perform any necessary cleanup
        return None



def insert_data_to_sql(df):
    """Following function inserts data to sql cloud database"""
    try:
        conn = engine.connect()
        transaction = conn.begin()
        
        insert_statement = """
            INSERT INTO Hotel (hotel_name, hotel_location, bed_rooms, latitude, longitude)
            SELECT :hotel_name, :hotel_location, :bed_rooms, :latitude, :longitude
            WHERE NOT EXISTS (
                SELECT 1 FROM Hotel 
                WHERE hotel_name = :hotel_name
                AND hotel_location = :hotel_location
            )
        """

        # Execute the INSERT statement in bulk
        conn.execute(insert_statement, df.to_dict(orient='records'))
        
        # Commit the transaction
        transaction.commit()

        # Close the connection
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle the exception or perform any necessary cleanup



def query_google_cloud_sql():
    """Following function executes the query"""
    try:
        # Create a SQLAlchemy engine with the connection string
        engine = create_engine(db_connection_string)

        # Perform a select query on a table
        query = "SELECT count(*) FROM Hotel"
        result = engine.execute(query)
    
        if len(df) == result:
            print("Data is inserted successfully")

        engine.dispose()
    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle the exception or perform any necessary cleanup


"""The following code creates tasks separately"""


task_check_table = PythonOperator(
    task_id="check_table", python_callable=check_table, dag=dag
)

task_data_processing = PythonOperator(
    task_id="data_processing", python_callable=data_processing, dag=dag
)


task_insert_data = PythonOperator(
    task_id="insert_data_to_sql",
    python_callable=insert_data_to_sql,
    op_kwargs={"df": task_data_processing.output},
    dag=dag,
)

task_read_sql_data = PythonOperator(
    task_id="query_google_cloud_sql", 
    python_callable=query_google_cloud_sql, 
    dag=dag
)

# Set the dependency between the tasks
(
    task_check_table 
    >> task_data_processing
    >> task_insert_data
    >> task_read_sql_data
)
