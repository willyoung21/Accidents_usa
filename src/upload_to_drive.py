from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import sys
import os

def upload_to_drive(file_path, file_name):
    # Authentication with Google
    gauth = GoogleAuth()

    # Load the credentials file from a specific path
    gauth.LoadClientConfigFile("../src/client_secret.json")  # Change the path here to your credentials file

    # Authenticate via the web browser
    gauth.LocalWebserverAuth()

    # Create Google Drive client
    drive = GoogleDrive(gauth)

    # Create the file to upload
    gfile = drive.CreateFile({'title': file_name})

    # Set the content of the file
    gfile.SetContentFile(file_path)

    # Upload the file
    gfile.Upload()

    print(f"The file '{file_name}' has been successfully uploaded to Google Drive.")

# Call the function with the path to your file
csv_file_path = os.path.abspath(os.path.join('../data/merged_data.csv'))
upload_to_drive(csv_file_path, 'merged_data.csv')
print("File uploaded successfully")