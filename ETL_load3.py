#### ETL to capture film data (name and release date) from URL and insert into source DB
import psycopg2
import os
import pandas as pd
from psycopg2 import sql
from pyspark.sql import SparkSession
import requests
import json
from psycopg2 import extras
#########################################################
# Function to get the current directory
def get_current_dir():
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        current_dir = os.getcwd()
    return current_dir
    
# Function to fetch data from the API
def fetch_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None    
#########################################################

if __name__ == "__main__":

    # Get the current directory of the script
    current_dir = get_current_dir()


    # Construct the relative path to the config file and JDBC jar
    config_path = os.path.join(current_dir, 'config', 'DBconfig.json')
    jar_path = os.path.join(current_dir, 'lib', 'db2jcc4.jar')

    # Load the configuration file
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Extract DB2 connection details
    target_db_config = config['source']
    target_db_config = config['target']
    #print(target_db_config)

    ################################################
    # Database connection parameters
    db_params = {
        'dbname': target_db_config['DBname'] ,
        'user':  target_db_config['user'] ,
        'password':  target_db_config['password'] ,
        'host':  target_db_config['host'] ,
        'port':  target_db_config['port'] 
    }


    # Initialize Spark Session
    spark = SparkSession.builder.appName("FilmsData").getOrCreate()



    # API URL for films
    api_url = "https://swapi.dev/api/films/"

    # Fetch films data
    films_data = fetch_data(api_url)

    # Extract 'results' from the response
    if films_data:
        films_list = films_data['results']
    else:
        films_list = []

    # Convert the list to an RDD and then to DataFrame
    films_rdd = spark.sparkContext.parallelize(films_list)
    films_df = spark.read.json(films_rdd)

    # Select required columns (title and created)'
    films_selected_df = films_df.select("title", "release_date","url")

    # Show the DataFrame
    #films_selected_df.show(truncate=False)


    # Establish connection
    try:
        conn = psycopg2.connect(**db_params)
        print("Connection established")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        exit()

    # Create a cursor object
    cursor = conn.cursor()


    '''
    -------------------------------------------------------------------
    '''
    try:
        print ('delete first')
        query = f"DELETE FROM DW.film_info WHERE 1=1;"
        
        # Execute the DELETE query
        cursor.execute(query)
        
        # Commit the transaction
        #conn.commit()
        print('delete completed ')
        print('started insert')
       
        films_load_df=films_selected_df.toPandas()

        table_name = 'DW.film_info'
        columns = ['film_name', 'release_date','film_url']
        
        # Create the SQL insert statement dynamically
        insert_query = sql.SQL("INSERT INTO DW.film_info  VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        # Prepare the data for insertion using itertuples
        values = [tuple(row) for row in films_load_df.itertuples(index=False)]
        
        # Use execute_values to perform bulk insert
        extras.execute_values(cursor, insert_query, values)
        
        # Commit the transaction
        conn.commit()    
        print('---------insert and commit completed----')
    except Exception as e:
        print(f"Error executing query: {e}")


    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
        # Stop the Spark session
        #spark.stop()
    print(' ETL load 3 completed ')    