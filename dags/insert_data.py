from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
import os
import logging
from dotenv import load_dotenv

# # Define your DAG
db_connection_string = "postgresql+psycopg2://Laiba_Naeem:qwerty@cloudsql:5433/laiba_db"


# Load the environment variables from .env file
# load_dotenv()

# # Retrieve the connection parameters from environment variables
# db_username = os.getenv("DB_USERNAME")
# db_password = os.getenv("DB_PASSWORD")
# db_port = os.getenv("DB_PORT")
# db_name = os.getenv("DB_NAME")

# # Create the connection string
# db_connection_string = f"postgresql+psycopg2://{db_username}:{db_password}@cloudsql:{db_port}/{db_name}"

# Define the DAG
dag = DAG(
    "csv_to_sql_dag",
    description="DAG to read CSV data and insert into the Hotel table",
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
)


# # Define the task
def database_connection():
    """This task creates the database connection with DB"""
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
    # engine.dispose()
def check_table():
    engine = create_engine(db_connection_string)
    query = """SELECT * FROM information_schema.tables WHERE table_name = 'Hotel' """
    result = engine.execute(query)
    print("result", result)
    engine.dispose()

def read_csv_data():
    """Following function reads the data from dataset"""
    # Get the absolute path of the DAGs directory
    dags_directory = os.path.abspath(os.path.dirname(__file__))

    # Construct the absolute path to the CSV file
    csv_file_path = os.path.join(dags_directory, "hotel_dataset.csv")

    # Read the CSV file
    df = pd.read_csv(csv_file_path)

    # Return the DataFrame
    return df

def data_cleaning():

    '''This function is used for data cleaning purpose'''
    dags_directory = os.path.abspath(os.path.dirname(__file__))

    # # Construct the absolute path to the CSV file
    csv_file_path = os.path.join(dags_directory, "hotel_dataset.csv")

    df = pd.read_csv(csv_file_path)
    df = df.loc[0:150]
    df = df.dropna(axis=0, subset=['id'])
    print(df.isnull().sum())
    print(df.head())
    return df

def insert_data_to_sql(df):
    """Following function inserts data to sql cloud database"""
    # Create a SQLAlchemy engine with the connection string
  
    engine = create_engine(db_connection_string)

    # Insert the data into the "Hotel" table
    df.to_sql("Hotel", engine, if_exists="append", index=False)

    # Close the database connection
    engine.dispose()


def query_google_cloud_sql():
    """Following function executes the query"""
    # Create a SQLAlchemy engine with the connection string
    engine = create_engine(db_connection_string)

    # Perform a select query on a table
    query = "SELECT * FROM Hotel"
    result = engine.execute(query)

    # Process the result
    for row in result:
        # Log each row in the Airflow logs
        logging.info(row)

    # Close the database connection
    engine.dispose()


task = PythonOperator(task_id="database_connection", python_callable=database_connection, dag=dag)

task_check_table = PythonOperator(task_id="check_table", python_callable=check_table, dag=dag)

task_read_csv = PythonOperator(task_id="read_csv_data", python_callable=read_csv_data, dag=dag)

task_data_cleaning = PythonOperator(task_id="data_cleaning", python_callable=data_cleaning, dag=dag)

task_insert_data = PythonOperator(task_id="insert_data_to_sql",python_callable=insert_data_to_sql,op_kwargs={"df": task_data_cleaning.output},
dag=dag)

task_read_sql_data = PythonOperator(task_id="query_google_cloud_sql", python_callable=query_google_cloud_sql, dag=dag)

# # Set the dependency between the tasks
task >> task_check_table >> task_read_csv >> task_data_cleaning >> task_insert_data >> task_read_sql_data
