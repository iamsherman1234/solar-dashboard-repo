import os
import json
import pandas as pd
import numpy as np
import io
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- CONFIGURATION ---
# PASTE YOUR FOLDER ID HERE
DRIVE_FOLDER_ID = '1jhw0IRHwG8ogRCL9g9Qu3RAsN0gkNLPI' 

# Paths (Using ".." to go up from src to root)
METADATA_FILE = os.path.join(os.path.dirname(__file__), '../data/sites_metadata.xlsx')
ADDITIONAL_INFO = os.path.join(os.path.dirname(__file__), '../data/additional_site_info.csv')
OUTPUT_HTML = 'index.html'

def get_drive_service():
    """Authenticate with Google Drive"""
    creds_json = os.environ.get('GDRIVE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GDRIVE_CREDENTIALS secret not found!")
    
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)

def download_monitoring_data(service):
    """Downloads files and uses Robust Header Detection"""
    print(f"Connecting to Drive Folder: {DRIVE_FOLDER_ID}...")
    try:
        results = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false",
            fields="files(id, name)"
        ).execute()
    except Exception as e:
        print(f"Error connecting to Drive: {e}")
        return pd.DataFrame()
    
    files = results.get('files', [])
    all_data = []
    print(f"Found {len(files)} monitoring files. Processing...")
    
    for file in files:
        try:
            print(f"  Downloading {file['name']}...")
            # Download file to memory
            request = service.files().get_media(fileId=file['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            # --- ROBUST HEADER DETECTION PATCH START ---
            header_row_index = 0
            
            # 1. Reset pointer to start
            fh.seek(0)
            
            # 2. Scan first 50 rows to find where data starts
            try:
                # Read without header first
                df_test = pd.read_excel(fh, header=None, nrows=50, engine='openpyxl')
                
                found_header = False
                for i, row in df_test.iterrows():
                    # Create list of strings for this row to check contents
                    row_values = [str(val).strip() for val in row.values]
                    
                    # Check for your specific keywords
                    if 'Site' in row_values and 'Solar Supply (kWh)' in row_values:
                        header_row_index = i
                        found_header = True
                        break
                
                if not found_header:
                    print(f"    ⚠ Could not find 'Site' and 'Solar Supply' headers in {file['name']}")
                    continue
                    
            except Exception as scan_error:
                print(f"    ⚠ Error scanning {file['name']}: {scan_error}")
                continue

            # 3. Reset pointer again to read the actual data
            fh.seek(0)
            
            # 4. Read the full file using the found header index
            df = pd.read_excel(fh, header=header_row_index, engine='openpyxl')
            
            # --- ROBUST HEADER DETECTION PATCH END ---

            # Clean columns (remove hidden characters/spaces)
            df.columns = [str(col).replace('\ufeff', '').strip() for col in df.columns]
            
            # Logic from Script 1: Extract Site, Date, Solar
            if 'Site' in df.columns and 'Date' in df.columns and 'Solar Supply (kWh)' in df.columns:
                
                # Handle "Site" vs "Site ID"
                if 'Site ID' in df.columns:
                    site_col = 'Site ID'
                else:
                    site_col = 'Site'
                
                temp_df = df[[site_col, 'Date', 'Solar Supply (kWh)']].copy()
                temp_df.columns = ['Site_ID', 'Date', 'Solar_kWh']
                
                # Conversions
                temp_df['Date'] = pd.to_datetime(temp_df['Date'], errors='coerce')
                temp_df['Solar_kWh'] = pd.to_numeric(temp_df['Solar_kWh'], errors='coerce')
                temp_df['Site_ID'] = temp_df['Site_ID'].astype(str).str.strip()
                
                # Drop rows with invalid dates
                temp_df = temp_df.dropna(subset=['Date'])
                
                all_data.append(temp_df)
            else:
                print(f"    ⚠ Missing required columns in {file['name']}")
                
        except Exception as e:
            print(f"Skipping {file['name']}: {e}")

    if not all_data:
        return pd.DataFrame()
        
    # Combine and Remove Duplicates
    combined = pd.concat(all_data, ignore_index=True)
    combined = combined.sort_values('Date').drop_duplicates(subset=['Site_ID', 'Date'], keep='last')
    
    # Pivot
    pivot_df = combined.pivot(index='Site_ID', columns='Date', values='Solar_kWh').reset_index()
    return pivot_df

def process_data(pivot_df):
    """Combines Metadata and calculates stats"""
    print("Loading Metadata and Calculating Stats...")
    
    if not os.path.exists(METADATA_FILE):
        print("Metadata file not found!")
        return pd.DataFrame(), []

    meta_df = pd.read_excel(METADATA_FILE)
    
    # Clean Site ID
    if 'Split' in meta_df.columns:
        meta_df['Site_ID'] = meta_df['Split'].astype(str).str.strip()
    else:
        meta_df['Site_ID'] = meta_df['Site'].astype(str).str.strip()

    # Calculate Array Size
    def calculate_array_size(row):
        try:
            return (float(row['Panels']) * float(row['Panel Size'])) / 1000
        except:
            return 0
    meta_df['Array_Size_kWp'] = meta_df.apply(calculate_array_size, axis=1)

    # Create Panel Description
    def create_desc(row):
        try:
            return f"{row['Panel Size']}W {row['Panel Vendor']} {row['Panel Model']}"
        except:
            return "Unknown"
    meta_df['Panel_Description'] = meta_df.apply(create_desc, axis=1)

    # Merge
    final_df = meta_df.merge(pivot_df, on='Site_ID', how='left')

    # Calculate Stats
    date_cols = [c for c in final_df.columns if isinstance(c, pd.Timestamp)]
    
    if date_cols:
        latest_date = max(date_cols)
        
        # Windows
        date_7d = latest_date - pd.Timedelta(days=7)
        date_30d = latest_date - pd.Timedelta(days=30)
        
        cols_7d = [c for c in date_cols if c >= date_7d]
        cols_30d = [c for c in date_cols if c >= date_30d]
        
        # 30-Day Stats
        final_df['Prod_30d_kWh'] = final_df[cols_30d].sum(axis=1)
        final_df['Avg_Daily_30d_kWh'] = final_df[cols_30d].mean(axis=1)
        final_df['Avg_Yield_30d_kWh_kWp'] = final_df['Avg_Daily_30d_kWh'] / final_df['Array_Size_kWp']
        
        # All Time
        final_df['Total_Production'] = final_df[date_cols].sum(axis=1)

    rename_map = {c: c.strftime('%Y-%m-%d') for c in date_cols}
    final_df = final_df.rename(columns=rename_map)
    str_date_cols = list(rename_map.values())
    
    return final_df, str_date_cols

def generate_html(df, date_cols):
    """Generates the Dashboard"""
    print("Generating HTML Dashboard...")
    
    total_sites = len(df)
    active_sites = df['Total_Production'].notna().sum()
    
    # Categorize
    excellent = df[df['Avg_Yield_30d_kWh_kWp'] > 4.5]
    good = df[(df['Avg_Yield_30d_kWh_kWp'] >= 3.5) & (df['Avg_Yield_30d_kWh_kWp'] <= 4.5)]
    fair = df[(df['Avg_Yield_30d_kWh_kWp'] >= 2.5) & (df['Avg_Yield_30d_kWh_kWp'] < 3.5)]
    poor = df[df['Avg_Yield_30d_kWh_kWp'] < 2.5]
    
    rows = ""
    for _, row in df.iterrows():
        site_name = row['Site'] if pd.notna(row['Site']) else row['Site_ID']
        yield_val = row['Avg_Yield_30d_kWh_kWp']
        
        color = "#e74c3c"
        if pd.notna(yield_val):
            if yield_val > 4.5: color = "#2ecc71"
            elif yield_val > 3.5: color = "#3498db"
            elif yield_val > 2.5: color = "#f1c40f"

        rows += f"""
        <tr class="border-b hover:bg-slate-50">
            <td class="p-3 border-l-4" style="border-left-color: {color}">
                <div class="font-bold">{site_name}</div>
                <div class="text-xs text-gray-500">{row['Site_ID']}</div>
            </td>
            <td class="p-3">{row['Array_Size_kWp']:.2f} kWp</td>
            <td class="p-3 font-mono">{row['Avg_Daily_30d_kWh']:.1f} kWh</td>
            <td class="p-3 font-bold" style="color:{color}">{yield_val:.2f}</td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Solar Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap" rel="stylesheet">
        <style>body {{ font-family: 'Inter', sans-serif; }}</style>
    </head>
    <body class="bg-slate-100 text-slate-800">
        <div class="bg-slate-900 text-white p-6 shadow-md">
            <div class="max-w-6xl mx-auto flex justify-between items-center">
                <div>
                    <h1 class="text-2xl font-bold">🌞 Solar Fleet Dashboard</h1>
                    <p class="text-slate-400 text-sm">Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
                </div>
                <div class="text-right">
                    <div class="text-3xl font-bold text-green-400">{active_sites} / {total_sites}</div>
                    <div class="text-xs uppercase tracking-wider text-slate-500">Active Sites</div>
                </div>
            </div>
        </div>

        <div class="max-w-6xl mx-auto mt-8 grid grid-cols-1 md:grid-cols-4 gap-4 px-4">
            <div class="bg-white p-4 rounded shadow border-l-4 border-green-500">
                <div class="text-gray-500 text-xs uppercase">Excellent (>4.5)</div>
                <div class="text-2xl font-bold">{len(excellent)} Sites</div>
            </div>
            <div class="bg-white p-4 rounded shadow border-l-4 border-blue-500">
                <div class="text-gray-500 text-xs uppercase">Good (3.5-4.5)</div>
                <div class="text-2xl font-bold">{len(good)} Sites</div>
            </div>
            <div class="bg-white p-4 rounded shadow border-l-4 border-yellow-400">
                <div class="text-gray-500 text-xs uppercase">Fair (2.5-3.5)</div>
                <div class="text-2xl font-bold">{len(fair)} Sites</div>
            </div>
            <div class="bg-white p-4 rounded shadow border-l-4 border-red-500">
                <div class="text-gray-500 text-xs uppercase">Poor (<2.5)</div>
                <div class="text-2xl font-bold">{len(poor)} Sites</div>
            </div>
        </div>

        <div class="max-w-6xl mx-auto mt-8 px-4 pb-12">
            <div class="bg-white rounded shadow overflow-hidden">
                <table class="w-full text-sm text-left">
                    <thead class="bg-slate-50 text-slate-500 uppercase text-xs">
                        <tr>
                            <th class="p-3">Site</th>
                            <th class="p-3">Capacity</th>
                            <th class="p-3">30-Day Daily Avg</th>
                            <th class="p-3">Specific Yield</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """
    
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard generated: {OUTPUT_HTML}")

def main():
    service = get_drive_service()
    pivot_df = download_monitoring_data(service)
    
    if not pivot_df.empty:
        final_df, date_cols = process_data(pivot_df)
        generate_html(final_df, date_cols)
    else:
        print("No data found to process.")

if __name__ == "__main__":
    main()
