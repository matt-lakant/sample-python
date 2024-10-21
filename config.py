import os
from dotenv import load_dotenv

# Load environment variable
env = os.getenv('ENVIRONMENT').lower()
load_dotenv(f'.env.{env}')

class Config:
    """Base configuration."""
    ## Database
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')

    ## File Space
    SPACE_NAME = os.getenv('SPACE_NAME')
    REGION_NAME = os.getenv('REGION_NAME')  # Change according to your region
    ACCESS_KEY = os.getenv('ACCESS_KEY')
    SECRET_KEY = os.getenv('SECRET_KEY')

class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    # Add other development-specific configurations here

class TestConfig(Config):
    """Test configuration."""
    DEBUG = False
    TESTING = True
    # Add other test-specific configurations here

class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    # Add other production-specific configurations here

def get_config():
    """Get the appropriate configuration based on the environment. Default is 'development'"""
    env = os.getenv('ENVIRONMENT').lower()
    print(f"Environment: {env}")

    if env == 'production':
        return ProductionConfig()
    elif env == 'test':
        return TestConfig()
    else:
        return DevelopmentConfig()
    