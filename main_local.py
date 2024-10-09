import os
import subprocess


# Call the other script and capture its output
# result = subprocess.run(['python3', 'Read_File_From_DOSpace.py'], capture_output=True, text=True)
result = subprocess.run(['python3', 'Read_File_From_Local.py'], capture_output=True, text=True)

# Prepare the response message
if result.returncode != 0:
    msg = f"Error running other script: {result.stderr}"
else:
    msg = f"Output from other script: {result.stdout.strip()}"


