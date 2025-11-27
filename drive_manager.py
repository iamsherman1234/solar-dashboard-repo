import os
import json
import shutil
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io
from pathlib import Path

# --- CONFIGURATION ---
SCOPES = ['https://www.googleapis.com/auth/drive']
FOLDER_MONITORING = '1ZCVjpjqZ5rnLBhCTZf2yeQbEOX9zeYCm' # 01_Monitoring_Data
FOLDER_ARCHIVES = '19AJmzhnlwXI78B0HTNX3mke8sMr-XK1G'   # 02_Archives
FOLDER_OUTPUT = '1jhw0lRHwG8ogRCL9g9Qu3RAsN0gkNLPl'     # Solar_Project_Master

def authenticate():
    creds_json = os.environ.get('GDRIVE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GDRIVE_CREDENTIALS not found in env variables")
    creds_dict = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

def download_monitoring_data(service):
    """Download all Excel files from Monitoring folder to local monitoring_data/"""
    print(f"Checking for new files in Folder ID: {FOLDER_MONITORING}")
    results = service.files().list(
        q=f"'{FOLDER_MONITORING}' in parents and trashed=false",
        fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])

    if not items:
        print("No new files found.")
        return []

    local_dir = Path("monitoring_data")
    local_dir.mkdir(exist_ok=True)

    downloaded_files = []
    for item in items:
        if 'spreadsheet' in item['mimeType'] or item['name'].endswith('.xlsx'):
            print(f"Downloading {item['name']}...")
            request = service.files().get_media(fileId=item['id'])
            fh = io.FileIO(local_dir / item['name'], 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            downloaded_files.append(item)
    return downloaded_files

def sync_archives(service, downloaded_files):
    """
    Check if files were moved to local Archives/. 
    If so, move them on Google Drive from Monitoring to Archives.
    """
    local_archive = Path("Archives")
    if not local_archive.exists():
        return

    # List files currently in local Archive folder
    archived_local_names = [f.name for f in local_archive.glob('*.xlsx')]

    for drive_file in downloaded_files:
        if drive_file['name'] in archived_local_names:
            print(f"Archiving on Drive: {drive_file['name']}")
            # Move file: Add Archive parent, Remove Monitoring parent
            file = service.files().update(
                fileId=drive_file['id'],
                addParents=FOLDER_ARCHIVES,
                removeParents=FOLDER_MONITORING,
                fields='id, parents'
            ).execute()

def upload_outputs(service):
    """Upload production Excel, HTML, and History Parquet to Output Folder"""
    print("Uploading outputs...")
    
    # Files to look for (using wildcards)
    files_to_upload = []
    files_to_upload.extend(list(Path('.').glob('installed_sites_production_*.xlsx')))
    files_to_upload.extend(list(Path('.').glob('installed_sites_dashboard_*.html')))
    
    # Also backup the history parquet so next run is fast
    if Path('monitoring_data_history.parquet').exists():
        files_to_upload.append(Path('monitoring_data_history.parquet'))

    for local_file in files_to_upload:
        print(f"Uploading {local_file.name}...")
        
        # Check if file exists in output folder (to update or replace)
        query = f"'{FOLDER_OUTPUT}' in parents and name = '{local_file.name}' and trashed=false"
        results = service.files().list(q=query, fields="files(id)").execute()
        existing_files = results.get('files', [])

        file_metadata = {'name': local_file.name, 'parents': [FOLDER_OUTPUT]}
        media = MediaFileUpload(str(local_file), resumable=True)

        if existing_files:
            # Update existing file
            service.files().update(
                fileId=existing_files[0]['id'],
                media_body=media
            ).execute()
            print(f"Updated existing {local_file.name}")
        else:
            # Create new file
            service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"Created new {local_file.name}")

def download_history(service):
    """Try to download history parquet from Output folder to speed up processing"""
    print("Looking for existing history parquet...")
    query = f"'{FOLDER_OUTPUT}' in parents and name = 'monitoring_data_history.parquet' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    
    if items:
        print("Downloading history cache...")
        request = service.files().get_media(fileId=items[0]['id'])
        fh = io.FileIO('monitoring_data_history.parquet', 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        print("History cache restored.")

if __name__ == "__main__":
    import sys
    action = sys.argv[1]
    
    srv = authenticate()
    
    if action == "pre":
        download_history(srv) # Get cache
        files = download_monitoring_data(srv) # Get new data
        # Save list of downloaded files to json to check later
        with open('downloaded_manifest.json', 'w') as f:
            json.dump(files, f)
            
    elif action == "post":
        # Load manifest
        if os.path.exists('downloaded_manifest.json'):
            with open('downloaded_manifest.json', 'r') as f:
                files = json.load(f)
            sync_archives(srv, files)
        
        upload_outputs(srv)
