####This script generates random customer data and insert into source DB 

import psycopg2
import pandas as pd
from psycopg2 import sql
from faker import Faker
import random
from decimal import Decimal
import os
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


# Generate a single row of data based on the column descriptions
def generate_row():
    row = {}
    for column, (col_type, sample) in column_descriptions.items():
        if col_type == "string":
            if column == "poster_content":
                row[column] = random.choice(poster_contents)
            elif column == "email":
                row[column] = fake.email()
            elif column == "sales_rep":
                row[column] = fake.email()
            elif column == "promo_code":
                #row[column] = fake.word()
                row[column] = random.choice(promo_codes)
            else:
                row[column] = fake.text(max_nb_chars=len(sample))
        elif col_type == "int":
            row[column] = random.randint(1, 1000)
        elif col_type == "decimal":
            row[column] = round(Decimal(random.uniform(0.1, 100.0)), 2)
    return row

# Generate multiple rows of data
def generate_data(num_rows):
    data = []
    for _ in range(num_rows):
        data.append(generate_row())
    return data

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
    soruce_db_config = config['source']
    #print(soruce_db_config)

    # Database connection parameters
    db_params = {
        'dbname': soruce_db_config['DBname'] ,
        'user':  soruce_db_config['user'] ,
        'password':  soruce_db_config['password'] ,
        'host':  soruce_db_config['host'] ,
        'port':  soruce_db_config['port'] 
    }
    #################################################

    # Define column descriptions
    column_descriptions = {
        "poster_content": ("string", "Millennium Falcon"),
        "quantity": ("int", 7),
        "price": ("decimal", 2.9),
        "email": ("string", "sally_skywalker@gmail.com"),
        "sales_rep": ("string", "tej@swposters.com"),
        "promo_code": ("string", "radio")
    }

    poster_contents = ['AA-9 Coruscant freighter',
                    'arc-170',
                    'A-wing',
                    'Banking clan frigte',
                    'Belbullab-22 starfighter',
                    'B-wing',
                    'Calamari Cruiser',
                    'CR90 corvette',
                    'Death Star',
                    'Droid control ship',
                    'EF76 Nebulon-B escort frigate',
                    'Executor',
                    'H-type Nubian yacht',
                    'Imperial shuttle',
                    'Jedi Interceptor',
                    'Jedi starfighter',
                    'J-type diplomatic barge',
                    'Millennium Falcon',
                    'Naboo fighter',
                    'Naboo Royal Starship',
                    'Naboo star skiff',
                    'Rebel transport',
                    'Republic Assault ship',
                    'Republic attack cruiser',
                    'Republic Cruiser',
                    'Scimitar',
                    'Sentinel-class landing craft',
                    'Slave 1',
                    'Solar Sailer',
                    'Star Destroyer',
                    'Theta-class T-2c shuttle',
                    'TIE Advanced x1',
                    'Trade Federation cruiser',
                    'V-wing',
                    'X-wing',
                    'Y-wing']

    promo_codes = ['Radio','TV ad','Facdbook Ad','X link add', 'youtube ad','Launch coupon codes',
    'Discount codes for online shopping',
    'Referral codes',
    'Holiday-specific codes',
    'Abandoned cart coupon codes',
    'Bundle discounts',
    'First purchase discounts',
    'Loyalty program deals',
    'Minimum purchase discount codes',
    'Exclusive social offers',
    'Influencer discounts',
    'Newsletter signup offer',
    'Offers for purchasing online',
    'Flash sale discount codes',
    'Free shipping codes']


    random_integer = random.randint(1, 10)


    fake = Faker()
    #############################################################333333

    # Generate 10 rows of data
    num_rows = 1000
    data = generate_data(num_rows)

    # Convert data to a DataFrame
    cust_data = pd.DataFrame(data)
    #print('--- here is customer data---')
    #print(cust_data)

     

    # Establish connection
    try:
        conn = psycopg2.connect(**db_params)
        print("Connection established")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        exit()

    # Create a cursor object
    cursor = conn.cursor()

    # Define the query to get all records from a table
    table_name = 'source.customer'
    
    ##################################
    query = f"DELETE FROM source.customer  WHERE 1=1;"
    
    # Execute the DELETE query
    cursor.execute(query)
    
    # Commit the transaction
    conn.commit()
    print('delete completed ')
    
    #####################################
    print('started insert')
    
    insert_query = sql.SQL("""
        INSERT INTO source.customer (poster_content,quantity,price,email,sales_rep,promo_code)
        VALUES (%s, %s, %s,%s, %s, %s)
    """).format(table=sql.Identifier(table_name))

    # Insert DataFrame rows into the PostgreSQL table
    for index, row in cust_data.iterrows():
        cursor.execute(insert_query, (row['poster_content'], row['quantity'], row['price'],row['email'], row['sales_rep'], row['promo_code']))


    # Commit the transaction
    conn.commit()

    print('---------insert and commit completed----')

    conn.close()
    
    print('---------ETL load 1 generate cust data and insert into source completed----')
    '''
    query = f"SELECT * FROM {table_name}"




    try:
        # Execute the query
        cursor.execute(query)

        # Fetch all rows from the executed query
        data = cursor.fetchall()

        # Get column names from cursor description
        column_names = [desc[0] for desc in cursor.description]

        # Load data into a pandas DataFrame
        df = pd.DataFrame(data, columns=column_names)

        # Print DataFrame
        #print(df)
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
        print('----------END -----------')
    '''