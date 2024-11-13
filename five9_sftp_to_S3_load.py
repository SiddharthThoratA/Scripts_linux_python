import json
import paramiko
import boto3
import os
import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from JSON file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# AWS and SFTP configuration
aws_config = config['aws']
sftp_config = config['sftp']

# Function to list CSV files from SFTP
def list_csv_files(sftp, path):
    return [file for file in sftp.listdir(path) if file.endswith('.csv')]

# Function to download file from SFTP
def download_file(sftp, remote_path, local_path, file_name):
    remote_file_path = os.path.join(remote_path, file_name)
    local_file_path = os.path.join(local_path, file_name)
    sftp.get(remote_file_path, local_file_path)
    return local_file_path

# Function to upload file to S3 in a dated folder
def upload_to_s3(s3_client, file_path, bucket, s3_folder, original_file_name):
    date_folder = datetime.datetime.now().strftime("%Y%m%d")
    #date_folder = '20240310'
    s3_key = os.path.join(s3_folder, date_folder, original_file_name)
    s3_client.upload_file(file_path, bucket, s3_key)
    logging.info(f'Uploaded {file_path} to S3 at {s3_key}')

# Function to create folder on SFTP
def create_folder_sftp(sftp, folder_path):
    try:
        sftp.mkdir(folder_path)
    except IOError:
        logging.warning(f'Folder {folder_path} already exists on SFTP server')

# Function to move file in SFTP
def move_file_sftp(sftp, original_path, new_path):
    try:
        sftp.rename(original_path, new_path)
    except IOError:
        logging.error(f'Error moving file from {original_path} to {new_path}')

# Main function
def main():
    # Set up SFTP connection
    transport = paramiko.Transport((sftp_config['host'], 22))
    transport.connect(username=sftp_config['username'], password=sftp_config['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Set up S3 client
    s3_client = boto3.client('s3', aws_access_key_id=aws_config['access_key'],
                             aws_secret_access_key=aws_config['secret_key'])

    # Process each CSV file
    for file_name in list_csv_files(sftp, sftp_config['remote_file_path']):
        local_file_path = download_file(sftp, sftp_config['remote_file_path'], '.', file_name)
        upload_to_s3(s3_client, local_file_path, aws_config['s3_bucket_name'], aws_config['s3_key_prefix'], file_name)
        
        # Create date-based folder on SFTP and move file
        date_folder_path = os.path.join(sftp_config['remote_file_path'], datetime.datetime.now().strftime("%Y%m%d"))
        #date_folder_path = os.path.join(sftp_config['remote_file_path'], '20240715')
        create_folder_sftp(sftp, date_folder_path)
        move_file_sftp(sftp, os.path.join(sftp_config['remote_file_path'], file_name), os.path.join(date_folder_path, file_name))

        os.remove(local_file_path)  # Remove the local file after uploading
        logging.info(f'Moved {file_name} to folder {date_folder_path} on SFTP')

    sftp.close()
    transport.close()
    print('state : complete')

if __name__ == "__main__":
    main()
