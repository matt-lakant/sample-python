import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Replace these variables with your own
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
SPACE_NAME = 'dev-filezone'
REGION_NAME = 'ams3'  # Change according to your region
FILE_NAME = 'FAZI-CLOSING-EN-2024-09-03.csv'  # The name of the file you want to read

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
        
        return file_content

    except NoCredentialsError:
        print("Credentials not available.")
    except ClientError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    content = read_file_from_space(SPACE_NAME, FILE_NAME)
    print(content)