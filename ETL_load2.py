###ETL script to colect starship and appearing film and load in target db 
from pyspark.sql import SparkSession
import requests
import json
import os 
import psycopg2
from pyspark.sql.functions import explode
from psycopg2 import sql
import psycopg2.extras as extras



# Initialize Spark Session
spark = SparkSession.builder.appName("StarshipData").getOrCreate()

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

# Function to extract starship data
def extract_starship_data(api_url):
    data = fetch_data(api_url)
    if not data:
        return []

    starships = data['results']
    while data['next']:
        data = fetch_data(data['next'])
        starships.extend(data['results'])
    
    return starships

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
    target_db_config = config['target']
    #print(target_db_config)

    ################################################

    # API URL for starships
    api_url = "https://swapi.dev/api/starships/"

    # Fetch starship data
    starships_data = extract_starship_data(api_url)

    # Convert to RDD and then to DataFrame
    starships_rdd = spark.sparkContext.parallelize(starships_data)
    starships_df = spark.read.json(starships_rdd)

    # Select required columns (name and films)
    starships_selected_df = starships_df.select("name", "films")

    # Show the DataFrame
    #starships_selected_df.show(truncate=False)
    #########################################################
    #########################################################
    # Function to get the current directory
    def get_current_dir():
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            current_dir = os.getcwd()
        return current_dir

    # Get the current directory of the script
    current_dir = get_current_dir()


    # Construct the relative path to the config file and JDBC jar
    config_path = os.path.join(current_dir, 'config', 'DBconfig.json')
    jar_path = os.path.join(current_dir, 'lib', 'db2jcc4.jar')

    # Load the configuration file
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Extract DB2 connection details
    target_db_config = config['target']
    #print(target_db_config)

    # Database connection parameters
    db_params = {
        'dbname': target_db_config['DBname'] ,
        'user':  target_db_config['user'] ,
        'password':  target_db_config['password'] ,
        'host':  target_db_config['host'] ,
        'port':  target_db_config['port'] 
    }
    #################################################
    ###############################################################
    # Establish connection
    try:
        conn = psycopg2.connect(**db_params)
        print("Connection established")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        exit()

    # Create a cursor object
    cursor = conn.cursor()

     
    try:
        print ('delete first')
        query = f"DELETE FROM  dw.starship_film   WHERE 1=1;"
        
        # Execute the DELETE query
        cursor.execute(query)
        
        # Commit the transaction
        #conn.commit()
        print('delete completed ')
        print('started insert')
       # Flatten the nested structure
        flattened_df = starships_selected_df.select("name", explode("films").alias("film"))

        starship_film_df=flattened_df.toPandas()

        table_name = 'dw.starship_film'
        columns = ['starship_name', 'film_name']
        
        # Create the SQL insert statement dynamically
        insert_query = sql.SQL("INSERT INTO dw.starship_film  VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        # Prepare the data for insertion using itertuples
        values = [tuple(row) for row in starship_film_df.itertuples(index=False)]
        
        # Use execute_values to perform bulk insert
        extras.execute_values(cursor, insert_query, values)
        
        # Commit the transaction
        conn.commit()    
        print('---------insert and commit completed----')
    except Exception as e:
        print(f"Error executing query: {e}")
    
    print("***********************     END of ETL Load 2  *****************************")
    #######################################################
    # Stop the Spark session
    #spark.stop()

