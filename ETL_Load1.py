import psycopg2
import os
import pandas as pd
from psycopg2 import sql
#from pyspark.sql import SparkSession
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
    soruce_db_config={}
    target_db_config={}
    target_db_config = config['target']
    #print(target_db_config)
    source_db_config = config['source']

    #print (source_db_config)
     
    soruce_db_config = config['source']
    #print(soruce_db_config)
     
    # Database connection parameters

    target_db_params = {
        'dbname': target_db_config['DBname'] ,
        'user':  target_db_config['user'] ,
        'password':  target_db_config['password'] ,
        'host':  target_db_config['host'] ,
        'port':  target_db_config['port'] 
    }
     
    source_db_params = {
        'dbname': soruce_db_config['DBname'] ,
        'user':  soruce_db_config['user'] ,
        'password':  soruce_db_config['password'] ,
        'host':  soruce_db_config['host'] ,
        'port':  soruce_db_config['port'] 
    }


     

    #################################################

    # Initialize Spark Session
    #spark = SparkSession.builder.appName("load source to target sales info").getOrCreate()
    ##############################################
     
    ###############################################################
    # Establish connection
    try:
        conn_source = psycopg2.connect(**source_db_params)
        print("Connection source established")
    except Exception as e:
        print(f"Error connecting to source database: {e}")
        exit()

    try:
        conn_target = psycopg2.connect(**target_db_params)
        print("Connection target established")
    except Exception as e:
        print(f"Error connecting to target  database: {e}")
        exit()
    # Create a cursor object
    cursor_source = conn_source.cursor()
    cursor_target = conn_target.cursor()
     
    # Define your query
    query = "select poster_content,quantity,price,sales_rep,promo_code from  source.customer "

    # Execute the query and read the data into a DataFrame
    df_source_data = pd.read_sql_query(query, conn_source)


    # Print the DataFrame
    #print(df_source_data)
    ############################################################################################333
     
    try:
        print ('delete first')
        query = f"DELETE FROM DW.sales_mst  WHERE 1=1;"
        
        # Execute the DELETE query
        cursor_target.execute(query)
        
        # Commit the transaction
        #conn_target.commit() 
        print('delete completed ')
        print('started insert')
       
        #films_load_df=films_selected_df.toPandas()

        table_name = 'DW.sales_mst'
        columns = ['poster_content','quantity','price','sales_rep','promo_code']
        
        # Create the SQL insert statement dynamically
        insert_query = sql.SQL("INSERT INTO DW.sales_mst VALUES %s  ").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        # Prepare the data for insertion using itertuples
        values = [tuple(row) for row in df_source_data.itertuples(index=False)]
        
        # Use execute_values to perform bulk insert
        extras.execute_values(cursor_target, insert_query, values)
        
        # Commit the transaction
        conn_target.commit()    
        print('---------insert and commit completed----')
    except Exception as e:
        print(f"Error executing query: {e}")


    finally:
        # Close the cursor and connection
        conn_source.close()
        cursor_source.close()
        conn_target.close()
        cursor_target.close()
        # Stop the Spark session
        #spark.stop()

    # Close the database connection soruce
    conn_source.close()
    cursor_source.close()
    conn_target.close()
    cursor_target.close()
    # Stop the Spark session
    #spark.stop()
    print("Data successfully copied to destination database.")
    print("***********************     END of ETL Load 1  *****************************")
