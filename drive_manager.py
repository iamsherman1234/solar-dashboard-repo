import os
import io
import shutil
from pathlib import Path
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# --- CONFIGURATION ---
FOLDER_MONITORING = '1ZCVjpjqZ5rnLBhCTZf2yeQbEOX9zeYCm' 
FOLDER_ARCHIVES = '19AJmzhnlwXI78B0HTNX3mke8sMr-XK1G'   
FOLDER_OUTPUT = '1jhw0lRHwG8ogRCL9g9Qu3RAsN0gkNLPl'     

def authenticate():
    """Authenticates using individual GitHub Secrets for User OAuth"""
    
    # Get secrets from environment variables
    client_id = os.environ.get('GDRIVE_CLIENT_ID')
    client_secret = os.environ.get('GDRIVE_CLIENT_SECRET')
    refresh_token = os.environ.get('GDRIVE_REFRESH_TOKEN')
    
    # Check if we are running locally with a token.json (Fallback for testing)
    if not all([client_id, client_secret, refresh_token]):
        if os.path.exists('token.json'):
            print("⚠ Using local token.json for authentication")
            creds = Credentials.from_authorized_user_file('token.json', ['https://www.googleapis.com/auth/drive'])
            return build('drive', 'v3', credentials=creds)
        else:
            raise ValueError("❌ Missing Environment Variables: GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET, or GDRIVE_REFRESH_TOKEN")

    print("✓ Authenticating as User via OAuth...")
    
    # Construct Credentials object manually
    creds = Credentials(
        None, # Access token (will be refreshed automatically)
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    
    return build('drive', 'v3', credentials=creds)

def download_monitoring_data(service):
    """Download new Excel files from Drive to local folder"""
    print("--- Checking Drive for new monitoring data ---")
    
    Path("monitoring_data").mkdir(exist_ok=True)
    Path("Archives").mkdir(exist_ok=True)

    results = service.files().list(
        q=f"'{FOLDER_MONITORING}' in parents and trashed=false",
        fields="files(id, name, mimeType)"
    ).execute()
    items = results.get('files', [])

    downloaded = []
    if not items:
        print("No new files found in Drive.")
    
    for item in items:
        # Check for Excel files
        if item['name'].endswith('.xlsx') or 'spreadsheet' in item['mimeType']:
            print(f"Downloading: {item['name']}")
            request = service.files().get_media(fileId=item['id'])
            fh = io.FileIO(f"monitoring_data/{item['name']}", 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                _, done = downloader.next_chunk()
            downloaded.append(item)
    
    return downloaded

def download_history(service):
    """Download history parquet"""
    print("--- Checking for Historical Cache ---")
    query = f"'{FOLDER_OUTPUT}' in parents and name = 'monitoring_data_history.parquet' and trashed=false"
    
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    
    if items:
        print("Downloading history parquet...")
        request = service.files().get_media(fileId=items[0]['id'])
        fh = io.FileIO('monitoring_data_history.parquet', 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            _, done = downloader.next_chunk()
        print("History restored.")
    else:
        print("No history file found (First run?).")

def sync_archives(service, downloaded_files):
    """Move processed files to Archive folder"""
    print("--- Syncing Archives ---")
    
    local_archive_path = Path("Archives")
    if not local_archive_path.exists(): 
        return

    archived_names = [f.name for f in local_archive_path.glob('*.xlsx')]

    for drive_file in downloaded_files:
        if drive_file['name'] in archived_names:
            print(f"Moving {drive_file['name']} to Archive on Drive...")
            try:
                # Move file by changing parents
                service.files().update(
                    fileId=drive_file['id'],
                    addParents=FOLDER_ARCHIVES,
                    removeParents=FOLDER_MONITORING,
                    fields='id, parents'
                ).execute()
            except Exception as e:
                print(f"Error moving file on Drive: {e}")

def upload_outputs(service):
    """Upload Results to Master Folder"""
    print("--- Uploading Outputs ---")
    
    files_to_upload = []
    files_to_upload.extend(list(Path('.').glob('installed_sites_production_*.xlsx')))
    files_to_upload.extend(list(Path('.').glob('installed_sites_dashboard_*.html')))
    if Path('monitoring_data_history.parquet').exists():
        files_to_upload.append(Path('monitoring_data_history.parquet'))

    for local_file in files_to_upload:
        print(f"Uploading: {local_file.name}")
        
        # Check if file exists (to overwrite)
        query = f"'{FOLDER_OUTPUT}' in parents and name = '{local_file.name}' and trashed=false"
        results = service.files().list(q=query, fields="files(id)").execute()
        existing = results.get('files', [])

        file_metadata = {'name': local_file.name, 'parents': [FOLDER_OUTPUT]}
        media = MediaFileUpload(str(local_file), resumable=True)

        if existing:
            # Update existing file
            service.files().update(
                fileId=existing[0]['id'],
                media_body=media
            ).execute()
            print(f"Updated existing file: {local_file.name}")
        else:
            # Create new file (Uses YOUR storage quota now)
            service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"Created new file: {local_file.name}")

if __name__ == "__main__":
    import sys
    # Default to 'pre' if no argument provided
    step = sys.argv[1] if len(sys.argv) > 1 else "pre"
    
    srv = authenticate()
    
    if step == "pre":
        download_history(srv)
        files = download_monitoring_data(srv)
        # Save list of downloaded files to json to check later
        with open('downloaded_files.json', 'w') as f:
            json.dump(files, f)
            
    elif step == "post":
        # Load manifest
        if os.path.exists('downloaded_files.json'):
            with open('downloaded_files.json', 'r') as f:
                files = json.load(f)
            sync_archives(srv, files)
        upload_outputs(srv)
