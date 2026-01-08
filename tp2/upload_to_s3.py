import os
import boto3
from datetime import datetime

# CONFIGURATION S3
S3_CONFIG = {
    'bucket': 'shopstream-datalake-imane',
    'region': 'eu-north-1'
}

# Dossier local contenant les fichiers à uploader
LOCAL_FOLDER = './csv_exports'  # Il reçoit ce dossier de toi

# Date de partition
today = datetime.now().strftime('%Y-%m-%d')

def upload_files_to_s3(local_folder):
    s3_client = boto3.client('s3', region_name=S3_CONFIG['region'])
    
    for file_name in os.listdir(local_folder):
        local_path = os.path.join(local_folder, file_name)
        if os.path.isfile(local_path):
            # Chemin S3 selon type de fichier
            if file_name.endswith('.csv'):
                table_name = file_name.replace('.csv', '')
                s3_key = f"raw/postgres/{table_name}/{today}/{file_name}"
            else:
                s3_key = f"raw/events/{today}/{file_name}"
            
            # Upload
            s3_client.upload_file(local_path, S3_CONFIG['bucket'], s3_key)
            print(f"Uploadé : {file_name} → s3://{S3_CONFIG['bucket']}/{s3_key}")

if __name__ == "__main__":
    upload_files_to_s3(LOCAL_FOLDER)
    print("Tous les fichiers ont été uploadés avec succès.")
