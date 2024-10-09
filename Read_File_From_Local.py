import os
import pandas as pd

# Define the path to the directory
directory_path = r"C:\Users\mattc\OneDrive - Lakant\Development\sample_data"

def read_csv_files_from_directory(directory):
    try:
        # List all files in the directory
        for filename in os.listdir(directory):
            # Construct full file path
            file_path = os.path.join(directory, filename)

            # Check if it's a file and ends with .txt (or .csv)
            if os.path.isfile(file_path) and filename.endswith(('.txt', '.csv')):
                # Load the file into a DataFrame with semicolon as delimiter
                df = pd.read_csv(file_path, delimiter=';')
                print(f"Contents of {filename}:\n{df}\n")
            else:
                print(f"{filename} is not a valid CSV file.")
            
            return df
    except Exception as e:
        print(f"Error reading files: {e}")

if __name__ == "__main__":
    df = read_csv_files_from_directory(directory_path)
    