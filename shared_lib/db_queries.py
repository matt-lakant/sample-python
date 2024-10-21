import os
from sqlalchemy import create_engine, text
import pandas as pd
import config # Import your configuration file

#####################################
# Database connection
#####################################
# Get the configuration for the current environment
config = config.get_config()

# Create a connection string using the config values
connection_string = f'postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}'

# Create a database engine
engine = create_engine(connection_string)

#####################################
# Utils
#####################################
def format_query(query, params):
    """Format the query with parameters for display purposes."""
    formatted_query = str(query)
    for key, value in params.items():
        # Use str() to convert the value to string representation
        formatted_query = formatted_query.replace(f":{key}", repr(value))
    return formatted_query

def query_to_dataframe(query, params=None):
    """Execute a SQL query and return the results as a DataFrame."""
    try:
        with engine.connect() as connection:
            if params:
                formatted_query = format_query(query, params)
                print("Executing Query:", formatted_query)
                result = connection.execute(query, params)
            else:
                result = connection.execute(query)
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

#####################################
# SQL queries
#####################################
def get_rows_radix_regex_query(data_provider_id):
    """Retrieve the list of file radix regular expressions for this data provider."""
    query = text(
        f"""
        SELECT index_file_radix_id, index_file_radix
        FROM dim_index_file_radix
        WHERE index_file_data_provider_id = :data_provider_id
        """
    )
    return query_to_dataframe(query, {'data_provider_id': data_provider_id})

def get_index_ts_id_query(index_ts_field_name, index_ts_identifier):
    """Retrieve the index time series's id corresponding to the identifier."""
    query = text(
        f"""
        SELECT index_ts_id
        FROM dim_index_ts
        WHERE {index_ts_field_name} = :index_ts_identifier
        """
    )
    return query_to_dataframe(query, {'index_ts_field_name': index_ts_field_name, 'index_ts_identifier': index_ts_identifier})

def get_list_index_file_fields(data_date, index_file_radix_id):
    """Retrieve list of fields for the file radix as of the date of the data (file fields might change over time)."""
    query = text(
        f"""
        SELECT	 if_field.index_file_field_name AS index_file_field_name
	    FROM    (
				SELECT   if_field2.index_file_radix_id
					    ,if_field2.index_file_field_name
					    ,if_field2.index_file_field_order_id
				FROM    dim_index_file_field if_field2
				JOIN (
					SELECT       MAX(apply_date) AS max_apply_date
						        ,if_field.index_file_radix_id AS index_file_radix_id
						        ,if_field.index_file_field_order_id AS index_file_field_order_id
					FROM        dim_index_file_field if_field
					WHERE       apply_date <= :data_date
					GROUP BY    if_field.index_file_radix_id, if_field.index_file_field_order_id
					) if_field1
				ON if_field2.apply_date = if_field1.max_apply_date
				AND if_field2.index_file_field_order_id = if_field1.index_file_field_order_id
				AND if_field2.index_file_radix_id = if_field1.index_file_radix_id
				) if_field
			    ,dim_index_file_radix AS if_radix
        WHERE	    if_field.index_file_radix_id = if_radix.index_file_radix_id
        AND         if_radix.index_file_radix_id = :index_file_radix_id
        ORDER BY    if_field.index_file_field_order_id
        """
    )
    df = query_to_dataframe(query, {'data_date': data_date, 'index_file_radix_id': index_file_radix_id})

    # Convert the DataFrame column to a list
    return df['index_file_field_name'].tolist()  # Extract the column and convert to a list


def get_user_by_id_dataframe(user_id):
    """Retrieve a user by ID as a DataFrame."""
    query = "SELECT * FROM users WHERE id = :id"
    return query_to_dataframe(text(query), {'id': user_id})

def add_user(name, email):
    """Add a new user to the database."""
    query = "INSERT INTO users (name, email) VALUES (:name, :email)"
    with engine.connect() as connection:
        connection.execute(text(query), {'name': name, 'email': email})