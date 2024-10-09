import os
import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError, ClientError

# Replace these variables with your own
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

SPACE_NAME = 'dev-filezone'
REGION_NAME = 'ams3'  # Change according to your region
FILE_NAME = 'FAZI-CLOSING-EN-2024-09-03.csv'  # The name of the file you want to read

if ACCESS_KEY is None or SECRET_KEY is None:
    print("Access Key or Secret Key is not set.")
else:
    print("Access Key and Secret Key are set.")

def read_file_from_space(space_name, file_name):
    try:
        # Create a session using the access key and secret key
        session = boto3.session.Session()
        client = session.client('s3',
                                region_name=REGION_NAME,
                                endpoint_url=f'https://{REGION_NAME}.digitaloceanspaces.com',
                                aws_access_key_id=ACCESS_KEY,
                                aws_secret_access_key=SECRET_KEY)

        # Read the file
        response = client.get_object(Bucket=space_name, Key=file_name)
        file_content = response['Body'].read().decode('utf-8')  # Read and decode the content
        
        # Load the content into a DataFrame with semicolon as delimiter
        from io import StringIO
        df = pd.read_csv(StringIO(file_content), delimiter=';')
        
        return df

    except NoCredentialsError:
        print("Credentials not available.")
    except ClientError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    df = read_file_from_space(SPACE_NAME, FILE_NAME)
    if df is not None:
        print(df)
    
    # Writing to database
    from sqlalchemy import create_engine

    # Database connection parameters
    DB_USER = os.getenv('DB_USER')  # Your database user
    DB_PASSWORD = os.getenv('DB_PASSWORD')  # Your database password
    DB_HOST = os.getenv('DB_HOST')  # Your database host (e.g., localhost)
    DB_PORT = os.getenv('DB_PORT')  # Your database port (usually 5432)
    DB_NAME = os.getenv('DB_NAME')  # Your database name

    # Create a connection string
    connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    # Create a SQLAlchemy engine
    engine = create_engine(connection_string)

    # Specify the name of the table
    table_name = 'your_table_name2'

    # Save the DataFrame to PostgreSQL, auto-creating the table if it doesn't exist
    try:
        df.to_sql(table_name, engine, index=False, if_exists='replace')  # Change 'replace' to 'append' if needed
        print(f"DataFrame saved to table '{table_name}' successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")