# data_ingestion.py

from sqlalchemy import create_engine, text
import logging
import pandas as pd

logger = logging.getLogger('data_ingestion')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
db_path = 'sqlite:///Maji_Ndogo_farm_survey_small.db'

sql_query = """
SELECT *
FROM geographic_features
LEFT JOIN weather_features USING (Field_ID)
LEFT JOIN soil_and_crop_features USING (Field_ID)
LEFT JOIN farm_management_features USING (Field_ID)
"""

weather_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv"
weather_mapping_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv"

def create_db_engine(db_path):
    """
    Create a database engine using SQLAlchemy.

    Parameters:
    db_path (str): The path to the SQLite database.

    Returns:
    engine: The SQLAlchemy engine object.

    Raises:
    ImportError: If SQLAlchemy is not installed.
    Exception: If failed to create the database engine.
    """
    try:
        engine = create_engine(db_path)
        with engine.connect() as conn:
            pass
        logger.info("Database engine created successfully.")
        return engine
    except ImportError:
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    Execute a SQL query using the provided engine.

    Parameters:
    engine: The SQLAlchemy engine object.
    sql_query (str): The SQL query to execute.

    Returns:
    DataFrame: A pandas DataFrame containing the results of the query.

    Raises:
    ValueError: If the query returns an empty DataFrame.
    Exception: If an error occurs while querying the database.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    Read a CSV file from a web URL.

    Parameters:
    URL (str): The URL pointing to the CSV file.

    Returns:
    DataFrame: A pandas DataFrame containing the data from the CSV file.

    Raises:
    pd.errors.EmptyDataError: If the URL does not point to a valid CSV file.
    Exception: If failed to read CSV from the web.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
