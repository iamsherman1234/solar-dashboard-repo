import os
import json
import pandas as pd
import numpy as np
import io
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# --- CONFIGURATION ---
MONITORING_FOLDER_ID = '1ZCVjpjqZ5rnLBhCTZf2yeQbEOX9zeYCm' 
ARCHIVE_FOLDER_ID    = '19AJmzhnlwXI78B0HTNX3mke8sMr-XK1G' 

# FILE NAMES
MASTER_FILENAME = 'all_monitoring_data.parquet'

# PATHS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
METADATA_FILE = os.path.join(BASE_DIR, 'data/sites_metadata.xlsx')
ADDITIONAL_INFO = os.path.join(BASE_DIR, 'data/additional_site_info.csv')
OUTPUT_HTML = 'index.html'

# PROVINCE MAPPING
PROVINCE_MAPPING = {
    'SV': 'Sihanoukville', 'KK': 'Koh Kong', 'SI': 'Siem Reap', 'PV': 'Prey Veng',
    'SR': 'Svay Rieng', 'KD': 'Kandal', 'KS': 'Kampong Speu', 'KC': 'Kampong Cham',
    'KH': 'Kampong Chhnang', 'BB': 'Battambang', 'PS': 'Pursat', 'PH': 'Preah Vihear',
    'KT': 'Kampong Thom', 'PL': 'Pailin', 'BM': 'Banteay Meanchey', 'TB': 'Tboung Khmum',
    'OM': 'Oddar Meanchey', 'KP': 'Kampot', 'KE': 'Kep', 'KR': 'Kratie',
    'ST': 'Stung Treng', 'MK': 'Mondulkiri', 'RK': 'Ratanakiri', 'PP': 'Phnom Penh', 'TK': 'Takeo'
}

def get_drive_service():
    creds_json = os.environ.get('GDRIVE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GDRIVE_CREDENTIALS secret not found!")
    creds_dict = json.loads(creds_json)
    # Scope updated to allow file management
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)

def sync_and_load_data(service):
    """
    1. Loads existing Master Parquet (if exists).
    2. Downloads and processes NEW .xlsx files.
    3. Combines data.
    4. Uploads updated Master Parquet back to Drive.
    5. Moves processed .xlsx files to Archive.
    """
    print("Scanning Drive folders...", flush=True)
    
    # List files in Monitoring Folder
    results = service.files().list(
        q=f"'{MONITORING_FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name)"
    ).execute()
    files = results.get('files', [])
    
    master_file_id = None
    new_excel_files = []
    
    for f in files:
        if f['name'] == MASTER_FILENAME:
            master_file_id = f['id']
        elif f['name'].endswith('.xlsx'):
            new_excel_files.append(f)

    # --- STEP 1: LOAD HISTORICAL MASTER ---
    master_df = pd.DataFrame()
    if master_file_id:
        print(f"Found Master Data ({MASTER_FILENAME}). Downloading...", flush=True)
        try:
            request = service.files().get_media(fileId=master_file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            fh.seek(0)
            master_df = pd.read_parquet(fh)
            print(f"Loaded {len(master_df)} historical records.", flush=True)
        except Exception as e:
            print(f"⚠ Error loading master parquet: {e}", flush=True)

    # --- STEP 2: PROCESS NEW EXCEL FILES ---
    new_data = []
    processed_files = [] # Keep track of files to move
    
    if new_excel_files:
        print(f"Found {len(new_excel_files)} new Excel files to process...", flush=True)
        
        for file in new_excel_files:
            print(f"Processing {file['name']}...", flush=True)
            try:
                request = service.files().get_media(fileId=file['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                
                # --- ROBUST HEADER LOGIC ---
                header_row_index = 0
                fh.seek(0)
                try:
                    df_test = pd.read_excel(fh, header=None, nrows=50, engine='openpyxl')
                    found_header = False
                    for i, row in df_test.iterrows():
                        row_values = [str(val).strip() for val in row.values]
                        if 'Site' in row_values and 'Solar Supply (kWh)' in row_values:
                            header_row_index = i
                            found_header = True
                            break
                    if not found_header:
                        print(f"  ⚠ Skipped {file['name']} (Header not found)", flush=True)
                        continue
                except: continue

                fh.seek(0)
                df = pd.read_excel(fh, header=header_row_index, engine='openpyxl')
                
                # Cleanup Columns
                df.columns = [str(col).replace('\ufeff', '').strip() for col in df.columns]
                
                if 'Site' in df.columns and 'Date' in df.columns and 'Solar Supply (kWh)' in df.columns:
                    site_col = 'Site ID' if 'Site ID' in df.columns else 'Site'
                    temp_df = df[[site_col, 'Date', 'Solar Supply (kWh)']].copy()
                    temp_df.columns = ['Site_ID', 'Date', 'Solar_kWh']
                    temp_df['Date'] = pd.to_datetime(temp_df['Date'], errors='coerce')
                    temp_df['Solar_kWh'] = pd.to_numeric(temp_df['Solar_kWh'], errors='coerce')
                    temp_df['Site_ID'] = temp_df['Site_ID'].astype(str).str.strip()
                    temp_df = temp_df.dropna(subset=['Date'])
                    
                    new_data.append(temp_df)
                    processed_files.append(file) # Mark for archiving
                else:
                    print(f"  ⚠ Missing columns in {file['name']}", flush=True)
                    
            except Exception as e:
                print(f"  ⚠ Error reading {file['name']}: {e}", flush=True)
    else:
        print("No new Excel files found.", flush=True)

    # --- STEP 3: COMBINE & DEDUPLICATE ---
    if not new_data and master_df.empty:
        return pd.DataFrame()
    
    combined_df = master_df
    if new_data:
        print("Merging new data...", flush=True)
        new_df = pd.concat(new_data, ignore_index=True)
        combined_df = pd.concat([master_df, new_df], ignore_index=True)
    
    # Deduplicate (Latest entry wins)
    if not combined_df.empty:
        combined_df = combined_df.sort_values('Date').drop_duplicates(subset=['Site_ID', 'Date'], keep='last')

    # --- STEP 4: UPLOAD UPDATED MASTER ---
    if new_data: # Only update Drive if we actually added something
        print("Uploading updated Master Parquet to Drive...", flush=True)
        # Save to local temp file first
        combined_df.to_parquet('temp_master.parquet', index=False)
        
        file_metadata = {'name': MASTER_FILENAME}
        media = MediaIoBaseUpload(io.BytesIO(open('temp_master.parquet', 'rb').read()), mimetype='application/octet-stream', resumable=True)
        
        if master_file_id:
            # Update existing
            service.files().update(fileId=master_file_id, media_body=media).execute()
        else:
            # Create new
            file_metadata['parents'] = [MONITORING_FOLDER_ID]
            service.files().create(body=file_metadata, media_body=media).execute()
        
        print("✅ Master Parquet Updated.", flush=True)

        # --- STEP 5: ARCHIVE PROCESSED FILES ---
        if ARCHIVE_FOLDER_ID and ARCHIVE_FOLDER_ID != 'PASTE_YOUR_ARCHIVE_FOLDER_ID_HERE':
            print(f"Archiving {len(processed_files)} files...", flush=True)
            for file in processed_files:
                try:
                    # Move file: Add Archive Parent, Remove Monitor Parent
                    service.files().update(
                        fileId=file['id'],
                        addParents=ARCHIVE_FOLDER_ID,
                        removeParents=MONITORING_FOLDER_ID,
                        fields='id, parents'
                    ).execute()
                    print(f"  Moved {file['name']} to Archive.", flush=True)
                except Exception as e:
                    print(f"  ⚠ Failed to archive {file['name']}: {e}", flush=True)
        else:
            print("⚠ Skipping Archive: ARCHIVE_FOLDER_ID not set in code.", flush=True)

    # Return Pivot for Dashboard Generation
    print("Pivoting data for dashboard...", flush=True)
    if combined_df.empty: return pd.DataFrame()
    
    pivot_df = combined_df.pivot(index='Site_ID', columns='Date', values='Solar_kWh').reset_index()
    return pivot_df

def process_data(pivot_df):
    print("Loading Metadata and Calculating Stats...", flush=True)
    if not os.path.exists(METADATA_FILE):
        print(f"Metadata not found at {METADATA_FILE}")
        return pd.DataFrame(), []

    meta_df = pd.read_excel(METADATA_FILE)
    if 'Split' in meta_df.columns:
        meta_df['Site_ID'] = meta_df['Split'].astype(str).str.strip()
    else:
        meta_df['Site_ID'] = meta_df['Site'].astype(str).str.strip()

    def calculate_array_size(row):
        try: return (float(row['Panels']) * float(row['Panel Size'])) / 1000
        except: return 0
    meta_df['Array_Size_kWp'] = meta_df.apply(calculate_array_size, axis=1)

    def create_desc(row):
        try: return f"{row['Panel Size']}W {row['Panel Vendor']} {row['Panel Model']}"
        except: return "Unknown"
    meta_df['Panel_Description'] = meta_df.apply(create_desc, axis=1)

    final_df = meta_df.merge(pivot_df, on='Site_ID', how='left')

    date_cols = [c for c in final_df.columns if isinstance(c, pd.Timestamp)]
    if date_cols:
        latest_date = max(date_cols)
        cols_7d = [c for c in date_cols if c >= latest_date - pd.Timedelta(days=7)]
        cols_30d = [c for c in date_cols if c >= latest_date - pd.Timedelta(days=30)]
        cols_90d = [c for c in date_cols if c >= latest_date - pd.Timedelta(days=90)]
        
        final_df['Prod_7d_kWh'] = final_df[cols_7d].sum(axis=1)
        final_df['Avg_Yield_7d_kWh_kWp'] = (final_df[cols_7d].mean(axis=1) / final_df['Array_Size_kWp']).fillna(0)
        
        final_df['Prod_30d_kWh'] = final_df[cols_30d].sum(axis=1)
        final_df['Avg_Yield_30d_kWh_kWp'] = (final_df[cols_30d].mean(axis=1) / final_df['Array_Size_kWp']).fillna(0)
        
        final_df['Prod_90d_kWh'] = final_df[cols_90d].sum(axis=1)
        final_df['Avg_Yield_90d_kWh_kWp'] = (final_df[cols_90d].mean(axis=1) / final_df['Array_Size_kWp']).fillna(0)
        
        final_df['Total_Production'] = final_df[date_cols].sum(axis=1)
        final_df['Days_With_Data'] = final_df[date_cols].notna().sum(axis=1)
        final_df['Avg_Daily_Production_kWh'] = final_df[date_cols].mean(axis=1)

        def get_first_date(row):
            for col in date_cols:
                if pd.notna(row[col]) and row[col] > 0: return col
            return None
        final_df['First_Production_Date'] = final_df.apply(get_first_date, axis=1)

    return final_df, date_cols

def generate_html(df, date_cols):
    print("Generating HTML...", flush=True)
    active_sites = df['Total_Production'].notna().sum()

    site_name_map = {}
    site_commissioned_map = {}
    if os.path.exists(ADDITIONAL_INFO):
        try:
            db_df = pd.read_csv(ADDITIONAL_INFO)
            if 'site_id' in db_df.columns:
                site_name_map = dict(zip(db_df['site_id'], db_df.get('site_name', db_df['site_id'])))
                site_commissioned_map = dict(zip(db_df['site_id'], db_df.get('commissioned_date', '')))
        except: pass

    def get_province(site_id):
        if isinstance(site_id, str) and len(site_id) >= 2: return PROVINCE_MAPPING.get(site_id[:2].upper(), site_id[:2])
        return 'Unknown'
    df['Province_Full'] = df['Site_ID'].apply(get_province)

    # --- DEGRADATION & CHARTS PREP ---
    degradation_data = []
    sorted_dates = sorted(date_cols) if date_cols else []
    
    for idx, row in df.iterrows():
        site_id = row['Site_ID']
        array_size = row['Array_Size_kWp']
        if pd.isna(array_size) or array_size == 0: continue
        
        first_date = row['First_Production_Date']
        if pd.isna(first_date): continue
        
        comm_start = first_date
        comm_end = first_date + pd.Timedelta(days=30)
        if not sorted_dates: continue
        latest_date = sorted_dates[-1]
        last_month_start = latest_date - pd.Timedelta(days=30)
        
        comm_cols = [c for c in sorted_dates if comm_start <= c < comm_end]
        last_cols = [c for c in sorted_dates if last_month_start <= c <= latest_date]
        
        if comm_cols and last_cols:
            comm_vals = [row[c] for c in comm_cols if pd.notna(row[c]) and row[c] > 0]
            last_vals = [row[c] for c in last_cols if pd.notna(row[c]) and row[c] > 0]
            
            if comm_vals and last_vals:
                initial_95th = np.percentile(comm_vals, 95) / array_size
                latest_95th = np.percentile(last_vals, 95) / array_size
                years_elapsed = (latest_date - first_date).days / 365.25
                
                if years_elapsed <= 1: expected = years_elapsed * 1.5
                else: expected = 1.5 + (years_elapsed - 1) * 0.4
                
                actual_deg = ((initial_95th - latest_95th) / initial_95th * 100) if initial_95th > 0 else 0
                perf_vs_exp = expected - actual_deg
                has_recent = any(pd.notna(row[d]) and row[d] > 0 for d in sorted_dates[-3:])

                degradation_data.append({
                    'site_id': site_id,
                    'site_name': site_name_map.get(site_id, row['Site']),
                    'array_size': array_size,
                    'panel_description': str(row['Panel_Description']),
                    'province': row['Province_Full'],
                    'initial_yield_95th': initial_95th,
                    'latest_yield_95th': latest_95th,
                    'years_elapsed': years_elapsed,
                    'expected_degradation': expected,
                    'actual_degradation': actual_deg,
                    'performance_vs_expected': perf_vs_exp,
                    'has_recent_data': has_recent
                })
    degradation_df = pd.DataFrame(degradation_data)

    grid_access_stats = df.groupby('Grid Access').agg(site_count=('Site_ID', 'count')).reset_index()
    power_sources_stats = df.groupby('Power Sources').agg(site_count=('Site_ID', 'count')).reset_index()
    
    comm_timeline = df[df['First_Production_Date'].notna()].copy()
    comm_timeline = comm_timeline.sort_values('First_Production_Date')
    comm_counts = comm_timeline.groupby('First_Production_Date').size().reset_index(name='count')
    comm_counts['cumulative_count'] = comm_counts['count'].cumsum()
    comm_counts['date_str'] = comm_counts['First_Production_Date'].dt.strftime('%Y-%m-%d')
    commissioning_data = comm_counts[['date_str', 'cumulative_count', 'count']].to_dict('records')

    site_data = {}
    for idx, row in df.iterrows():
        site_id = row['Site_ID']
        daily = []
        for d in date_cols:
            if pd.notna(row[d]):
                daily.append({
                    'date': d.strftime('%Y-%m-%d'),
                    'solar_supply_kwh': float(row[d]),
                    'specific_yield': float(row[d]) / row['Array_Size_kWp'] if row['Array_Size_kWp'] > 0 else 0
                })
        
        site_data[site_id] = {
            'site_id': site_id,
            'site_name': site_name_map.get(site_id, str(row['Site'])),
            'project': str(row.get('Project', 'N/A')),
            'grid_access': str(row.get('Grid Access', 'N/A')),
            'power_sources': str(row.get('Power Sources', 'N/A')),
            'panel_description': str(row['Panel_Description']),
            'array_size_kwp': float(row['Array_Size_kWp']),
            'province': row['Province_Full'],
            'avg_yield_30d': float(row.get('Avg_Yield_30d_kWh_kWp', 0)),
            'commissioned_date': site_commissioned_map.get(site_id, str(row['First_Production_Date'])),
            'daily_data': daily
        }

    excellent = df[df['Avg_Yield_30d_kWh_kWp'] > 4.5].to_dict('records')
    good = df[(df['Avg_Yield_30d_kWh_kWp'] >= 3.5) & (df['Avg_Yield_30d_kWh_kWp'] <= 4.5)].to_dict('records')
    fair = df[(df['Avg_Yield_30d_kWh_kWp'] >= 2.5) & (df['Avg_Yield_30d_kWh_kWp'] < 3.5)].to_dict('records')
    poor = df[df['Avg_Yield_30d_kWh_kWp'] < 2.5].to_dict('records')

    crit_alerts = []
    last_3 = sorted_dates[-3:] if len(sorted_dates) >= 3 else sorted_dates
    for idx, row in df.iterrows():
        if all(pd.isna(row[d]) or row[d] == 0 for d in last_3):
            crit_alerts.append(row['Site_ID'])

    prov_stats = df.groupby('Province_Full').agg(
        site_count=('Site_ID', 'count'),
        total_capacity=('Array_Size_kWp', 'sum'),
        avg_yield=('Avg_Yield_30d_kWh_kWp', 'mean')
    ).reset_index().sort_values('avg_yield', ascending=False)
    
    proj_stats = df.groupby('Project').agg(
        site_count=('Site_ID', 'count'),
        total_capacity=('Array_Size_kWp', 'sum'),
        avg_yield=('Avg_Yield_30d_kWh_kWp', 'mean')
    ).reset_index().sort_values('avg_yield', ascending=False)
    
    panel_stats = df.groupby('Panel_Description').agg(
        site_count=('Site_ID', 'count'),
        total_capacity=('Array_Size_kWp', 'sum'),
        avg_yield=('Avg_Yield_30d_kWh_kWp', 'mean')
    ).reset_index().sort_values('avg_yield', ascending=False)

    # --- HTML STRINGS ---
    def gen_list(s, color, cat):
        name = site_name_map.get(s['Site_ID'], s['Site'])
        return f'''<div class="site-list-item" onclick="openSiteModal('{s['Site_ID']}', '{cat}')" style="cursor:pointer; padding:0.75rem; border-left:3px solid {color}; margin-bottom:0.5rem; background:#f8f9fa; border-radius:0.5rem;">
            <div style="display:flex; justify-content:space-between;"><strong>{name}</strong><span style="color:{color}">{s['Avg_Yield_30d_kWh_kWp']:.2f} kWh/kWp</span></div>
            <div style="font-size:0.8em; color:gray;">{s['Panel_Description']} • {s['Array_Size_kWp']:.1f} kWp</div></div>'''

    exc_html = ''.join([gen_list(s, '#27ae60', 'excellent') for s in excellent])
    good_html = ''.join([gen_list(s, '#3498db', 'good') for s in good])
    fair_html = ''.join([gen_list(s, '#f39c12', 'fair') for s in fair])
    poor_html = ''.join([gen_list(s, '#e74c3c', 'poor') for s in poor])

    def gen_stat_card(label, val, sub, color_code):
        color = {'green':'#27ae60', 'blue':'#3498db', 'yellow':'#f39c12', 'red':'#e74c3c'}.get(color_code, '#333')
        return f'''<div class="stat-card" style="border-left: 4px solid {color}; padding: 1rem; margin-bottom:0.5rem; background:white; border-radius:0.5rem; box-shadow:0 1px 3px rgba(0,0,0,0.1);">
            <div style="font-weight:bold; font-size:0.9em; color:#666;">{label}</div>
            <div style="font-size:1.5rem; font-weight:bold;">{val}</div>
            <div style="font-size:0.8rem; color:gray;">{sub}</div>
        </div>'''

    prov_html = ''.join([gen_stat_card(r['Province_Full'], f"{r['avg_yield']:.2f}", f"{r['site_count']} sites", 'blue' if r['avg_yield']>3.5 else 'yellow') for _, r in prov_stats.iterrows()])
    proj_html = ''.join([gen_stat_card(r['Project'], f"{r['avg_yield']:.2f}", f"{r['site_count']} sites", 'green' if r['avg_yield']>4.0 else 'yellow') for _, r in proj_stats.iterrows()])
    panel_html = ''.join([gen_stat_card(r['Panel_Description'], f"{r['avg_yield']:.2f}", f"{r['site_count']} sites", 'blue') for _, r in panel_stats.iterrows()])

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Solar Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif; background: #f8f9fa; color: #333; }}
        .header {{ background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); color: white; padding: 1.5rem 2rem; display: flex; align-items: center; justify-content: space-between; }}
        .nav {{ background: #e9ecef; border-bottom: 1px solid #dee2e6; padding: 0 2rem; display: flex; gap: 2rem; }}
        .nav-item {{ padding: 1rem 0.5rem; cursor: pointer; border-bottom: 3px solid transparent; font-weight: 500; color: #6c757d; }}
        .nav-item.active {{ color: #3498db; border-bottom-color: #3498db; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 2rem; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1.5rem; }}
        .card {{ background: white; border-radius: 0.75rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); padding: 1.5rem; }}
        .stat-value {{ font-size: 2.25rem; font-weight: bold; margin-top: 0.5rem; }}
        .hidden {{ display: none; }}
        .modal {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0, 0, 0, 0.7); z-index: 1000; overflow-y: auto; padding: 1rem; }}
        .modal.active {{ display: flex; align-items: center; justify-content: center; }}
        .modal-content {{ background: white; border-radius: 0.75rem; width: 100%; max-width: 1200px; max-height: 90vh; overflow-y: auto; padding: 2rem; }}
        .time-period-selector {{ display: flex; gap: 0.5rem; margin-bottom: 1rem; background: #e9ecef; padding: 0.375rem; border-radius: 0.5rem; }}
        .period-button {{ flex: 1; padding: 0.5rem; border: none; background: white; border-radius: 0.375rem; cursor: pointer; }}
        .period-button.active {{ background: #3498db; color: white; }}
        .chart-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin-top: 1rem; }}
        @media (max-width: 768px) {{ .chart-row {{ grid-template-columns: 1fr; }} }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🌞 Solar Performance Dashboard</h1>
        <div>{len(df)} Sites • {df['Array_Size_kWp'].sum():.1f} kWp</div>
    </div>
    <div class="nav">
        <div class="nav-item active" onclick="showTab('overview')">Overview</div>
        <div class="nav-item" onclick="showTab('sites')">All Sites</div>
        <div class="nav-item" onclick="showTab('degradation')">Degradation</div>
        <div class="nav-item" onclick="showTab('performance')">Performance</div>
    </div>
    
    <div class="container" id="overview">
        <div class="grid" style="margin-bottom: 2rem;">
            <div class="card" style="border-left: 4px solid #3498db;"><div>Total Sites</div><div class="stat-value">{len(df)}</div><div class="text-sm text-gray-500">{active_sites} active</div></div>
            <div class="card" style="border-left: 4px solid #f39c12;"><div>Avg Yield (30d)</div><div class="stat-value">{df['Avg_Yield_30d_kWh_kWp'].mean():.2f}</div><div class="text-sm text-gray-500">kWh/kWp/day</div></div>
            <div class="card" style="border-left: 4px solid #e74c3c;"><div>Critical Alerts</div><div class="stat-value">{len(crit_alerts)}</div><div class="text-sm text-gray-500">Zero production</div></div>
            <div class="card" style="border-left: 4px solid #27ae60;"><div>Excellent Sites</div><div class="stat-value">{len(excellent)}</div><div class="text-sm text-gray-500">> 4.5 kWh/kWp</div></div>
        </div>
        <div class="card" style="margin-bottom: 2rem;"><h3>Fleet Composition</h3><div class="chart-row"><div style="height: 300px;"><canvas id="gridChart"></canvas></div><div style="height: 300px;"><canvas id="powerChart"></canvas></div></div></div>
        <div class="card"><h3>Commissioning Timeline</h3><div style="height: 300px;"><canvas id="commChart"></canvas></div></div>
    </div>
    
    <div class="container hidden" id="sites">
        <div class="grid"><div class="card"><h3>🌟 Excellent</h3><div style="max-height:500px; overflow-y:auto">{exc_html}</div></div><div class="card"><h3>✅ Good</h3><div style="max-height:500px; overflow-y:auto">{good_html}</div></div><div class="card"><h3>⚠️ Fair</h3><div style="max-height:500px; overflow-y:auto">{fair_html}</div></div><div class="card"><h3>🚨 Poor</h3><div style="max-height:500px; overflow-y:auto">{poor_html}</div></div></div>
    </div>
    
    <div class="container hidden" id="degradation">
        <div class="card"><h3>Degradation Analysis</h3><div id="deg-list"></div></div>
    </div>
    
    <div class="container hidden" id="performance">
        <div class="grid"><div class="card"><h3>Province</h3>{prov_html}</div><div class="card"><h3>Project</h3>{proj_html}</div><div class="card"><h3>Panel</h3>{panel_html}</div></div>
    </div>
    
    <div id="site-modal" class="modal">
        <div class="modal-content">
            <div style="display:flex; justify-content:space-between; margin-bottom:1rem;"><h2 id="modal-title">Site Details</h2><button onclick="document.getElementById('site-modal').classList.remove('active')">Close</button></div>
            <div class="time-period-selector">
                <button class="period-button" onclick="loadSiteData(this, '7d')">7d</button>
                <button class="period-button" onclick="loadSiteData(this, '30d')">30d</button>
                <button class="period-button active" onclick="loadSiteData(this, '90d')">90d</button>
                <button class="period-button" onclick="loadSiteData(this, 'all')">All</button>
            </div>
            <div id="modal-info" style="display:grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap:1rem; margin-bottom:1rem; background:#f8f9fa; padding:1rem;"></div>
            <div class="chart-row"><div style="height:300px;"><canvas id="dailyChart"></canvas></div><div style="height:300px;"><canvas id="yieldChart"></canvas></div></div>
        </div>
    </div>

    <script>
        const siteData = {json.dumps(site_data)};
        const degData = {json.dumps(degradation_df.to_dict('records') if not degradation_df.empty else [])};
        const gridData = {json.dumps(grid_access_stats.to_dict('records'))};
        const powerData = {json.dumps(power_sources_stats.to_dict('records'))};
        const commData = {json.dumps(commissioning_data)};
        let currentSiteId = null;
        let siteCharts = [];

        function showTab(id) {{
            document.querySelectorAll('.container').forEach(d => d.classList.add('hidden'));
            document.getElementById(id).classList.remove('hidden');
            document.querySelectorAll('.nav-item').forEach(d => d.classList.remove('active'));
            event.target.classList.add('active');
        }}

        new Chart(document.getElementById('gridChart'), {{type: 'pie', data: {{labels: gridData.map(d => d['Grid Access']), datasets: [{{data: gridData.map(d => d.site_count), backgroundColor: ['#3498db', '#27ae60', '#f39c12', '#e74c3c']}}]}}, options: {{maintainAspectRatio: false}} }});
        new Chart(document.getElementById('powerChart'), {{type: 'pie', data: {{labels: powerData.map(d => d['Power Sources']), datasets: [{{data: powerData.map(d => d.site_count), backgroundColor: ['#e74c3c', '#3498db', '#f39c12', '#9b59b6']}}]}}, options: {{maintainAspectRatio: false}} }});
        new Chart(document.getElementById('commChart'), {{type: 'line', data: {{labels: commData.map(d => d.date_str), datasets: [{{label: 'Cumulative Sites', data: commData.map(d => d.cumulative_count), borderColor: '#3498db', fill: true}}]}}, options: {{maintainAspectRatio: false, scales: {{y: {{beginAtZero: true}}}}}} }});

        const degList = document.getElementById('deg-list');
        degData.sort((a,b) => b.actual_degradation - a.actual_degradation);
        let degHtml = '';
        degData.forEach(s => {{
            let color = s.actual_degradation > 50 ? '#e74c3c' : (s.actual_degradation > 30 ? '#f39c12' : '#27ae60');
            degHtml += `<div class="site-list-item" onclick="openSiteModal('${{s.site_id}}')" style="cursor:pointer; padding:1rem; border-left:4px solid ${{color}}; background:#f8f9fa; margin-bottom:0.5rem;">
                <div style="font-weight:bold; display:flex; justify-content:space-between;">${{s.site_name}}<span style="color:${{color}}">${{s.actual_degradation.toFixed(1)}}% Degraded</span></div>
                <div style="font-size:0.9em; color:#666;">Expected: ${{s.expected_degradation.toFixed(1)}}% • Age: ${{s.years_elapsed.toFixed(1)}} yrs</div>
            </div>`;
        }});
        degList.innerHTML = degHtml || 'Insufficient data.';

        window.openSiteModal = function(id) {{
            currentSiteId = id;
            const s = siteData[id];
            if(!s) return;
            document.getElementById('site-modal').classList.add('active');
            document.getElementById('modal-title').innerText = s.site_name;
            document.getElementById('modal-info').innerHTML = `<div>Province: ${{s.province}}</div><div>Capacity: ${{s.array_size_kwp}} kWp</div><div>Panel: ${{s.panel_description}}</div><div>Grid: ${{s.grid_access}}</div><div>Comm: ${{s.commissioned_date}}</div>`;
            loadSiteData(document.querySelectorAll('.period-button')[2], '90d');
        }};

        window.loadSiteData = function(btn, period) {{
            document.querySelectorAll('.period-button').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            const s = siteData[currentSiteId];
            if(!s) return;
            const days = {{'7d':7, '30d':30, '90d':90}};
            let filtered = s.daily_data;
            if(period !== 'all') {{
                const cutoff = new Date();
                cutoff.setDate(cutoff.getDate() - days[period]);
                filtered = s.daily_data.filter(d => new Date(d.date) >= cutoff);
            }}
            siteCharts.forEach(c => c.destroy());
            siteCharts = [];
            siteCharts.push(new Chart(document.getElementById('dailyChart'), {{type: 'bar', data: {{labels: filtered.map(d => d.date), datasets: [{{label: 'Production', data: filtered.map(d => d.solar_supply_kwh), backgroundColor: '#3498db'}}]}}, options: {{maintainAspectRatio: false}} }}));
            siteCharts.push(new Chart(document.getElementById('yieldChart'), {{type: 'line', data: {{labels: filtered.map(d => d.date), datasets: [{{label: 'Specific Yield', data: filtered.map(d => d.specific_yield), borderColor: '#27ae60', fill: true}}]}}, options: {{maintainAspectRatio: false}} }}));
        }};
    </script>
</body>
</html>"""
    
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Dashboard generated: {OUTPUT_HTML}", flush=True)

def main():
    service = get_drive_service()
    pivot_df = sync_and_load_data(service) # NEW LOGIC HERE
    
    if not pivot_df.empty:
        final_df, date_cols = process_data(pivot_df)
        generate_html(final_df, date_cols)
    else:
        print("No data found to process.", flush=True)

if __name__ == "__main__":
    main()
