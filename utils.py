from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import json
import os 


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
'''
db_params = {
    "dbname": "defaultdb",
    "user": "avnadmin",
    "password": "AVNS_2uZ9x3_vF9bnLNEmFdv",
    "host": "pg-20d77a1a-samplesource.e.aivencloud.com",
    "port": "26689"
}
'''
 
#####################################################################3
def get_film_year_release (film_name ):
    """
        This function will return the release date of a given film name 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    vfilmName=film_name
    try:
        # Create an engine
        engine = create_engine(db_url)
        #query = ''' select  film_name ,release_date from dw.film_info where film_name = {vfilmName} '''

        # Establish a connection
        with engine.connect() as connection:
            # Execute the provided query
            result = connection.execute(f("select   release_date from dw.film_info where film_name = {film_name}") )
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None

##############################################################
def get_avg_max_min_qty_single_purchase ( ):
    """
        This function will return average and maximum and minimum sales quantity of entire sales 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select round( avg(quantity)) avg_qty ,max(quantity) max_qty ,min(quantity)  min_qty from dw.sales_starship_film_view '''

        # Establish a connection
        with engine.connect() as connection:
            # Execute the provided query
            result = connection.execute(text(query))
            connection.close()
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None

####################################################################
def get_film_year_release ( ):
    """
        This function will return film names and its release date 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select  film_name,release_date from dw.film_info '''

        # Establish a connection
        with engine.connect() as connection:
            # Execute the provided query
            result = connection.execute(text(query))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None

#############################################################        
def sales_total_revenue ( ):
    """
        This function will return the total retenue generated 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select round(sum(price *quantity)) total_revenue from dw.sales_mst '''

        # Establish a connection
        with engine.connect() as connection:
            # Execute the provided query
            result = connection.execute(text(query))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None

###############################################################
def sales_revenue_by_rep ( rep_email):
    """
        This function will return the revenue made by a given sales rep mail id
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select round(sum(price *quantity)) as  revenue from dw.sales_mst where sales_rep='{}' '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format(rep_email)
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None

########################################################3
def poseters_sold_by_rep ( rep_email):
    """
            This function will return all posters that are sold by given sales rep emil id 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select  distinct poster_content from dw.sales_mst where sales_rep='{}' '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format(rep_email)
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None

#################################################################33

def get_most_used_promo_code ( ):
    """
        This function will return the most used promo code based on units sold 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select promo_code from (select promo_code ,count(*)  from dw.sales_mst group by promo_code order by 2 desc fetch first 1 rows only )'''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None

##########################################################3
def poster_qty_sold_by_rep ( rep_email):
    """
        This function will return total sales done by a given sales rep 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select  sum(quantity) as total_qty from dw.sales_mst where sales_rep='{}' '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format(rep_email)
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
#####################################################3
def get_most_sold_poster_content (  ):
    """
        This function will return the most sold poster content with its quantity of sales 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select poster_content,sum(quantity) total_qty  from dw.sales_mst group by poster_content order by 2 desc fetch first 1 rows only  '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
###################################################
def get_top10_salesrep_by_poster_qty (  ):
    """
        This function will return top 10 sales rep with their sales quantity
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select sales_rep ,sum(quantity) total_qty from dw.sales_mst group by sales_rep  order by 2 desc  fetch first 10 rows only  '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
#########################################################
def get_top10_salesrep_by_revenue  (  ):
    """
        This function will return top 10 sales rep along with revenue they generate 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select sales_rep ,round(sum(price * quantity)) total_revenue  from dw.sales_mst group by sales_rep  order by 2 desc  fetch first 10 rows only  '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
############################################################
def get_top10_posters_by_revenue  (  ):
    """
            This function will return top 10 posters with revenue  by the revenue it generated

    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select poster_content,round(sum(price *quantity)) total_revenue from dw.sales_mst group by poster_content  order by 2 desc fetch first 10 rows only  '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
############################################################3
def get_top10_posters_by_sales_qty  (  ):
    """
        This function will return top 10 poster content by sales quantity
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select poster_content, sum( quantity) total_qty  from dw.sales_mst group by poster_content   order by 2 desc fetch first 10 rows only  '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None

#############################################################3
def get_top10_promo_code_by_sales  (  ):
    """
        This function will return the top 10 promocodes that made the sales irrespective of quantity and revenue it generated 
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = '''select promo_code,count(*) total_count from dw.sales_mst group by promo_code  order by 2 desc fetch first 10 rows only  '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
#####################################################################
def get_promo_code_and_sales_qty  (  ):
    """
        This function will return all promo codes with quantity of sales it made
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = '''select promo_code,sum(quantity) total_qty from dw.sales_mst group by promo_code  order by 2 desc  '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
#######################################################################
def get_promo_code_and_revenue  (  ):
    """
        This function will return all promo codes with the revenues that it generate
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = '''select promo_code, round(sum(quantity * price )) total_revenue  from dw.sales_mst group by promo_code order by 2 desc   '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
###########################################################
def get_order_most_appearing_starship (  ):
    """
        This function will order the starships that is used in most movies
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = '''select starship_name,count(distinct film_name) film_name  from dw.starship_film_view  group by starship_name order by 2 desc  '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
####################################################33
def get_order_film_by_most_used_starship (  ):
    """
        This function will retun movies that used starships
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = '''select film_name,count(distinct  starship_name) starship_count from dw.starship_film_view   group by film_name order by 2 desc '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
###########################################################
def get_all_starships_appear_in_a_movie  ( movie_name):
    """
     This function will give all starships that appear in the movie that is given as input
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select starship_name from dw.starship_film_view  where  film_name ='{}' '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format(movie_name)
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
###################################################################3
def get_all_films_starship_appear   ( starship_name):
    """
        This function will take starship name and return all the films where this ship appears
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = ''' select film_name from dw.starship_film_view  where  starship_name  ='{}' '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format(starship_name)
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
#########################################################################
def get_polupar_films_ordered_by_posters_sold (  ):
    """
        This function will get all the films and sum of poster sold quantity and order by desc. So top films with 
        most posters sold will appear at top
    """
    # Construct the database URL
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create an engine
        engine = create_engine(db_url)
        query = '''select  film_name,sum(quantity) total_quantity from dw.sales_starship_film_view group by  film_name order by 2 desc  '''

        # Establish a connection
        with engine.connect() as connection:
            # Replace placeholders in the query with the single parameter
            query_formatted = query.format( )
            
            # Execute the formatted query
            result = connection.execute(text(query_formatted))
            
            # Fetch the result into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            connection.close()
            return df
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        return None
        
