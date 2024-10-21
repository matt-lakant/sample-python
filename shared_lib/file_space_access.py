import os
import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError, ClientError
from io import StringIO
from contextlib import contextmanager
import config # Import your configuration file

#####################################
# File space connection
#####################################
# Get the configuration for the current environment
config = config.get_config()

#####################################
# Context manager for S3 client
#####################################
@contextmanager
def s3_client_manager():
    """Context manager for creating and handling a Digital Ocean Spaces client."""
    
    if config.ACCESS_KEY is None or config.SECRET_KEY is None:
        print("Access Key or Secret Key is not set.")
        yield None  # Yield None if keys are not set
        return

    print("Access Key and Secret Key are set.")
    
    try:
        session = boto3.session.Session()
        client = session.client(
            's3',
            region_name=config.REGION_NAME,
            endpoint_url=f'https://{config.REGION_NAME}.digitaloceanspaces.com',
            aws_access_key_id=config.ACCESS_KEY,
            aws_secret_access_key=config.SECRET_KEY
        )
        yield client  # Yield the client to the caller
    except NoCredentialsError:
        print("Credentials not available.")
        yield None
    except ClientError as e:
        print(f"Error creating Spaces client: {e}")
        yield None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        yield None

#####################################
# Function to read first N lines from the file
#####################################
def read_first_n_lines_from_file(data_provider_mnemo, data_datetimed_file_name, n):
    """Read the first N lines from a file in the file space."""
    with s3_client_manager() as client:  # Use the context manager
        # The context manager handles all related errors and yields None if there's an issue.
        
        # Construct the full file path (including the folder)
        file_fullpath = f'{data_provider_mnemo}/Processing/{data_datetimed_file_name}'  # Add the folder path
        print("file_fullpath: ", file_fullpath)

        try:
            response = client.get_object(Bucket=config.SPACE_NAME, Key=file_fullpath)
            file_stream = response['Body']
            
            # Read the first n lines from the stream
            first_n_lines = []
            for _ in range(n):
                line = file_stream.readline().decode('utf-8')
                if not line:  # Break if there are fewer than n lines
                    break
                first_n_lines.append(line)

            # Load the content into a DataFrame with semicolon as delimiter
            df = pd.read_csv(StringIO(''.join(first_n_lines)), delimiter=';', header=None)
            
            return df

        except ClientError as e:
            print(f"Error accessing the file: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while reading the file: {e}")
