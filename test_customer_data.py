# This program will check the source DB and target DB customer data which is loaded By ETL job
import pytest
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.orm import sessionmaker
import os
import json

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

# Extract DB connection details
source_db_config = config['source']
target_db_config = config['target']

# Database connection details
source_user = source_db_config['user']
source_pass = source_db_config['password']
source_host = source_db_config['host']
source_port = source_db_config['port']
source_db = source_db_config['DBname']

target_user = target_db_config['user']
target_pass = target_db_config['password']
target_host = target_db_config['host']
target_port = target_db_config['port']
target_db = target_db_config['DBname']

# Create engines for both databases
source_engine = create_engine(f'postgresql+psycopg2://{source_user}:{source_pass}@{source_host}:{source_port}/{source_db}')
target_engine = create_engine(f'postgresql+psycopg2://{target_user}:{target_pass}@{target_host}:{target_port}/{target_db}')

# Create sessions for both databases
SourceSession = sessionmaker(bind=source_engine)
TargetSession = sessionmaker(bind=target_engine)

metadata = MetaData()

# Define the source.customer table
source_customer_table = Table('customer', metadata,
    autoload_with=source_engine, schema='source'
)

# Define the dw.sales_mst table
sales_mst_table = Table('sales_mst', metadata,
    autoload_with=target_engine, schema='dw'
)

@pytest.fixture(scope='function')
def source_db_session():
    connection = source_engine.connect()
    transaction = connection.begin()
    session = SourceSession(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture(scope='function')
def target_db_session():
    connection = target_engine.connect()
    transaction = connection.begin()
    session = TargetSession(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()

def fetch_data_from_query(connection, query):
    result = connection.execute(text(query))
    rows = result.fetchall()
    columns = result.keys()  # Fetch column names

    # Convert rows to a set of tuples
    return set(
        tuple(row[i] for i in range(len(columns)))
        for row in rows
    )

@pytest.fixture(scope='function')
def prepare_test_data(source_db_session, target_db_session):
    # Insert test data into source.customer
    source_db_session.execute(text("""
    INSERT INTO source.customer (poster_content, quantity, price, sales_rep, promo_code)
    VALUES ('poster1', 10, 19.99, 'rep1', 'promo1')
    """))
    
    # Insert test data into dw.sales_mst
    target_db_session.execute(text("""
    INSERT INTO dw.sales_mst (poster_content, quantity, price, sales_rep, promo_code)
    VALUES ('poster1', 10, 19.99, 'rep1', 'promo1')
    """))
    
    # Commit changes
    source_db_session.commit()
    target_db_session.commit()

    yield

    # Cleanup data
    source_db_session.execute(text("""
    DELETE FROM source.customer
    WHERE poster_content = 'poster1'
    """))
    
    target_db_session.execute(text("""
    DELETE FROM dw.sales_mst
    WHERE poster_content = 'poster1'
    """))
    
    source_db_session.commit()
    target_db_session.commit()

def test_data_consistency(source_db_session, target_db_session, prepare_test_data):
    source_query = """
    SELECT poster_content, quantity, price, sales_rep, promo_code
    FROM source.customer
    """
    
    target_query = """
    SELECT poster_content, quantity, price, sales_rep, promo_code
    FROM dw.sales_mst
    """

    source_data = fetch_data_from_query(source_db_session.bind, source_query)
    target_data = fetch_data_from_query(target_db_session.bind, target_query)
    
    # Ensure all data in source is in target
    for data in source_data:
        assert data in target_data, f"Data {data} from source.customer not found in dw.sales_mst"
    
    # Ensure all data in target is in source
    for data in target_data:
        assert data in source_data, f"Data {data} from dw.sales_mst not found in source.customer"

if __name__ == '__main__':
    pytest.main(['-v'])  # -v for verbose
