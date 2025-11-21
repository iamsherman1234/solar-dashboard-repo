import os
import json
import pandas as pd
import io
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- CONFIGURATION ---
# PASTE YOUR ID FOR '01_Monitoring_Data' BELOW
DRIVE_FOLDER_ID = '1ZCVjpjqZ5rnLBhCTZf2yeQbEOX9zeYCm' 

METADATA_FILE = 'data/sites_metadata.xlsx'
OUTPUT_HTML = 'index.html'

def get_drive_service():
    """Authenticate with Google Drive using GitHub Secrets"""
    creds_json = os.environ.get('GDRIVE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GDRIVE_CREDENTIALS secret not found! Did you add it to Settings?")
    
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)

def download_monitoring_data(service):
    print(f"Connecting to Folder ID: {DRIVE_FOLDER_ID}...")
    try:
        results = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false",
            fields="files(id, name)"
        ).execute()
    except Exception as e:
        print(f"Error Accessing Drive: {e}")
        return pd.DataFrame()
    
    files = results.get('files', [])
    all_data = []
    print(f"Found {len(files)} files. Downloading...")
    
    for file in files:
        try:
            request = service.files().get_media(fileId=file['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            fh.seek(0)
            
            df = pd.read_excel(fh)
            df.columns = df.columns.str.strip()
            
            # Basic extraction logic (Matches your original logic)
            if 'Site' in df.columns and 'Solar Supply (kWh)' in df.columns:
                # Use Site ID if present, else Site
                site_col = 'Site ID' if 'Site ID' in df.columns else 'Site'
                
                temp_df = df[[site_col, 'Date', 'Solar Supply (kWh)']].copy()
                temp_df.columns = ['Site_ID', 'Date', 'Solar_kWh']
                temp_df['Date'] = pd.to_datetime(temp_df['Date'], errors='coerce')
                temp_df['Solar_kWh'] = pd.to_numeric(temp_df['Solar_kWh'], errors='coerce')
                temp_df = temp_df.dropna(subset=['Date'])
                
                # Clean Site ID
                temp_df['Site_ID'] = temp_df['Site_ID'].astype(str).str.strip()
                
                all_data.append(temp_df)
                print(f"Loaded {file['name']}")
        except Exception as e:
            print(f"Skipping {file['name']}: {e}")

    if not all_data:
        return pd.DataFrame()
        
    combined = pd.concat(all_data, ignore_index=True)
    # Remove duplicates, keep latest
    return combined.sort_values('Date').drop_duplicates(subset=['Site_ID', 'Date'], keep='last')

def generate_dashboard(monitoring_df):
    print("Processing Data...")
    
    # Load Metadata
    if not os.path.exists(METADATA_FILE):
        print(f"Warning: {METADATA_FILE} not found in GitHub repo.")
        return
        
    meta = pd.read_excel(METADATA_FILE)
    # Ensure we match on Site_ID (assumes 'Split' column exists as per your file)
    if 'Split' in meta.columns:
        meta['Site_ID'] = meta['Split'].astype(str).str.strip()
    else:
        meta['Site_ID'] = meta['Site'].astype(str).str.strip()
    
    # Calculate Stats
    stats = monitoring_df.groupby('Site_ID')['Solar_kWh'].agg(['sum', 'mean', 'count']).reset_index()
    stats.columns = ['Site_ID', 'Total_Production', 'Avg_Daily', 'Days_Count']
    
    # Merge
    final_df = meta.merge(stats, on='Site_ID', how='left')
    
    # HTML Generation
    print("Generating HTML...")
    
    active_sites = final_df['Total_Production'].notna().sum()
    total_kwh = final_df['Total_Production'].sum()
    
    # Generate Rows
    table_rows = ""
    for _, row in final_df.iterrows():
        prod = row['Total_Production']
        prod_str = f"{prod:,.1f}" if pd.notna(prod) else "0.0"
        avg_str = f"{row['Avg_Daily']:.1f}" if pd.notna(row['Avg_Daily']) else "0.0"
        site_name = row['Site'] if pd.notna(row['Site']) else row['Site_ID']
        
        # Status dot color
        status_color = "#2ecc71" if pd.notna(prod) and prod > 0 else "#e74c3c"
        
        table_rows += f"""
        <tr class="hover:bg-gray-50 border-b border-gray-100">
            <td class="py-3 px-4 flex items-center gap-3">
                <span style="height: 10px; width: 10px; background-color: {status_color}; border-radius: 50%; display: inline-block;"></span>
                {site_name}
            </td>
            <td class="py-3 px-4 text-gray-500">{row['Site_ID']}</td>
            <td class="py-3 px-4 font-medium">{prod_str} kWh</td>
            <td class="py-3 px-4">{avg_str} kWh</td>
        </tr>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Solar Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-slate-50 text-slate-800">
        <div class="bg-blue-900 text-white p-6 shadow-lg">
            <div class="max-w-4xl mx-auto">
                <h1 class="text-2xl font-bold mb-2">🌞 Solar Site Monitor</h1>
                <div class="flex gap-6 text-blue-100 text-sm">
                    <span>📡 Active Sites: <strong>{active_sites}</strong></span>
                    <span>⚡ Total Yield: <strong>{total_kwh:,.0f} kWh</strong></span>
                </div>
            </div>
        </div>

        <div class="max-w-4xl mx-auto -mt-4 p-4">
            <div class="bg-white rounded-lg shadow-md overflow-hidden">
                <div class="overflow-x-auto">
                    <table class="w-full text-sm text-left">
                        <thead class="text-xs text-gray-700 uppercase bg-gray-50 border-b">
                            <tr>
                                <th class="px-4 py-3">Site Name</th>
                                <th class="px-4 py-3">ID</th>
                                <th class="px-4 py-3">Total Prod</th>
                                <th class="px-4 py-3">Daily Avg</th>
                            </tr>
                        </thead>
                        <tbody>
                            {table_rows}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <div class="text-center text-gray-400 text-xs py-8">
            Last Updated via GitHub Actions
        </div>
    </body>
    </html>
    """
    
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Success! Dashboard generated at {OUTPUT_HTML}")

def main():
    print("Starting Dashboard Update...")
    service = get_drive_service()
    df = download_monitoring_data(service)
    if not df.empty:
        generate_dashboard(df)
    else:
        print("No Excel files found in the specific Drive folder.")

if __name__ == "__main__":
    main()
