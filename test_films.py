# This program will check the films and release date loaded in DB with those available in URL
import pytest
import requests
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, select, cast
from sqlalchemy.orm import sessionmaker
import os
import json

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

# Construct the relative path to the config file
config_path = os.path.join(current_dir, 'config', 'DBconfig.json')

# Load the configuration file
with open(config_path, 'r') as f:
    config = json.load(f)

# Extract DB2 connection details
target_db_config = config['target']

# Database connection details
username = target_db_config['user']
password = target_db_config['password']
host = target_db_config['host']
port = target_db_config['port']
db_name = target_db_config['DBname']

# Create engine and session
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}')
Session = sessionmaker(bind=engine)

metadata = MetaData()

# Define the table with the correct schema and column names
#films_table = Table('film_info', metadata, autoload_with=engine, schema='dw')                    
films_table = Table('film_info', metadata,
                    Column('film_name', String),
                    Column('release_date', Date),
                    schema='dw'
                    )
def fetch_film_data():
    response = requests.get('https://swapi.dev/api/films/')
    if response.status_code == 200:
        return response.json()['results']
    return []

@pytest.fixture(scope='module')
def db_session():
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()

def test_film_data_matches(db_session):
    # Fetch film data from SWAPI
    films_from_api = fetch_film_data()
  
    # Query to fetch film data from the database (corrected line)
    query = select(
        films_table.c.film_name,
        cast(films_table.c.release_date, String)
    )
   
    result = db_session.execute(query).fetchall()
  
    # Extract film data from database results
    films_from_db = [{'film_name': row[0], 'release_date': row[1]} for row in result]
  
    # Verify that film data matches between API and database
    for film_api in films_from_api:
        found = False
        for film_db in films_from_db:
            if film_api['title'] == film_db['film_name'] and film_api['release_date'] == film_db['release_date']:
                found = True
                break
        assert found, f"Film {film_api['title']} not found in database with matching release date"

if __name__ == '__main__':
    pytest.main(['-v'])  # -v for verbose output
