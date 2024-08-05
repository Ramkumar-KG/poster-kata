
# This program will check the starships loaded in the DB with those available in URL 
import pytest
import requests
from sqlalchemy import create_engine, MetaData, Table, Column, String, select,text
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
starships_table = Table('starship_film', metadata,
    Column('starship_name', String),
    autoload_with=engine, schema='dw'
)

def fetch_starships():
    response = requests.get('https://swapi.dev/api/starships/')
    if response.status_code == 200:
        return [starship['name'] for starship in response.json()['results']]
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

def test_starships_names_correct(db_session):
  # Fetch starship names from the API
  starship_names_from_api = fetch_starships()
  
  # Query to fetch starship names from the database (use alias)
  raw_query = text("SELECT starship_name   FROM dw.starship_film")
  result = db_session.execute(raw_query).fetchall()

  
  # Extract starship names from database results
  starship_names_from_db = {row[0] for row in result}
  
  # Verify that all starship names from the API are in the database
  for name in starship_names_from_api:
    assert name in starship_names_from_db, f"Starship {name} not found in database"




if __name__ == '__main__':
    pytest.main(['-v'])  # -v for verbose
