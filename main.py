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
ARCHIVE_FOLDER_ID    = 'PASTE_YOUR_ARCHIVE_FOLDER_ID_HERE' 

# FILE NAMES
MASTER_FILENAME = 'all_monitoring_data.parquet'

# PATHS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
METADATA_FILE = os.path.join(BASE_DIR, 'data/sites_metadata.xlsx')
ADDITIONAL_INFO = os.path.join(BASE_DIR, 'data/additional_site_info.csv')
OUTPUT_HTML = 'index.html'
OUTPUT_JSON = 'dashboard_data.json' # New separate data file

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
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)

def sync_and_load_data(service):
    # ... (This function remains EXACTLY the same as before) ...
    # For brevity, I'm not repeating the full sync logic here, 
    # but in your actual file, keep the full content of sync_and_load_data
    print("Scanning Drive folders...", flush=True)
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

    new_data = []
    processed_files = []
    
    if new_excel_files:
        print(f"Found {len(new_excel_files)} new Excel files...", flush=True)
        for file in new_excel_files:
            print(f"Processing {file['name']}...", flush=True)
            try:
                request = service.files().get_media(fileId=file['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                
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
                    processed_files.append(file)
                else:
                    print(f"  ⚠ Missing columns in {file['name']}", flush=True)
            except Exception as e:
                print(f"  ⚠ Error reading {file['name']}: {e}", flush=True)

    if not new_data and master_df.empty:
        return pd.DataFrame()
    
    combined_df = master_df
    if new_data:
        print("Merging new data...", flush=True)
        new_df = pd.concat(new_data, ignore_index=True)
        combined_df = pd.concat([master_df, new_df], ignore_index=True)
    
    if not combined_df.empty:
        combined_df = combined_df.sort_values('Date').drop_duplicates(subset=['Site_ID', 'Date'], keep='last')

    if new_data:
        print("Uploading updated Master Parquet...", flush=True)
        combined_df.to_parquet('temp_master.parquet', index=False)
        
        file_metadata = {'name': MASTER_FILENAME}
        media = MediaIoBaseUpload(io.BytesIO(open('temp_master.parquet', 'rb').read()), mimetype='application/octet-stream', resumable=True)
        
        if master_file_id:
            service.files().update(fileId=master_file_id, media_body=media).execute()
        else:
            file_metadata['parents'] = [MONITORING_FOLDER_ID]
            service.files().create(body=file_metadata, media_body=media).execute()
        
        print("✅ Master Updated.", flush=True)

        if ARCHIVE_FOLDER_ID and ARCHIVE_FOLDER_ID != 'PASTE_YOUR_ARCHIVE_FOLDER_ID_HERE':
            print(f"Archiving {len(processed_files)} files...", flush=True)
            for file in processed_files:
                try:
                    service.files().update(fileId=file['id'], addParents=ARCHIVE_FOLDER_ID, removeParents=MONITORING_FOLDER_ID).execute()
                except: pass

    print("Pivoting data...", flush=True)
    if combined_df.empty: return pd.DataFrame()
    pivot_df = combined_df.pivot(index='Site_ID', columns='Date', values='Solar_kWh').reset_index()
    return pivot_df

def process_data(pivot_df):
    # ... (Keep existing process_data function exactly the same) ...
    print("Processing Stats...", flush=True)
    if not os.path.exists(METADATA_FILE): return pd.DataFrame(), []

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
        cols_30d = [c for c in date_cols if c >= latest_date - pd.Timedelta(days=30)]
        
        final_df['Prod_30d_kWh'] = final_df[cols_30d].sum(axis=1)
        final_df['Avg_Yield_30d_kWh_kWp'] = (final_df[cols_30d].mean(axis=1) / final_df['Array_Size_kWp']).fillna(0)
        final_df['Total_Production'] = final_df[date_cols].sum(axis=1)

        def get_first_date(row):
            for col in date_cols:
                if pd.notna(row[col]) and row[col] > 0: return col
            return None
        final_df['First_Production_Date'] = final_df.apply(get_first_date, axis=1)

    return final_df, date_cols

def generate_html(df, date_cols):
    print("Generating HTML...", flush=True)
    active_sites = df['Total_Production'].notna().sum()

    # ... (Keep metadata loading logic) ...
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

    # --- DEGRADATION CALCULATION ---
    # ... (Keep existing degradation logic) ...
    degradation_data = []
    sorted_dates = sorted(date_cols) if date_cols else []
    
    for idx, row in df.iterrows():
        site_id = row['Site_ID']
        array_size = row['Array_Size_kWp']
        if pd.isna(array_size) or array_size == 0: continue
        first_date = row['First_Production_Date']
        if pd.isna(first_date): continue
        
        latest_date = sorted_dates[-1]
        comm_cols = [c for c in sorted_dates if first_date <= c < first_date + pd.Timedelta(days=30)]
        last_cols = [c for c in sorted_dates if latest_date - pd.Timedelta(days=30) <= c <= latest_date]
        
        if comm_cols and last_cols:
            comm_vals = [row[c] for c in comm_cols if pd.notna(row[c]) and row[c] > 0]
            last_vals = [row[c] for c in last_cols if pd.notna(row[c]) and row[c] > 0]
            
            if comm_vals and last_vals:
                initial_95th = np.percentile(comm_vals, 95) / array_size
                latest_95th = np.percentile(last_vals, 95) / array_size
                years_elapsed = (latest_date - first_date).days / 365.25
                expected = 1.5 + (years_elapsed - 1) * 0.4 if years_elapsed > 1 else years_elapsed * 1.5
                actual_deg = ((initial_95th - latest_95th) / initial_95th * 100)
                
                degradation_data.append({
                    'site_id': site_id,
                    'site_name': site_name_map.get(site_id, row['Site']),
                    'actual_degradation': round(actual_deg, 1),
                    'expected_degradation': round(expected, 1),
                    'years_elapsed': round(years_elapsed, 1)
                })
    degradation_df = pd.DataFrame(degradation_data)

    # --- AGGREGATIONS ---
    grid_access_stats = df.groupby('Grid Access').agg(site_count=('Site_ID', 'count')).reset_index()
    power_sources_stats = df.groupby('Power Sources').agg(site_count=('Site_ID', 'count')).reset_index()
    
    comm_timeline = df[df['First_Production_Date'].notna()].sort_values('First_Production_Date')
    comm_counts = comm_timeline.groupby('First_Production_Date').size().reset_index(name='count')
    comm_counts['cumulative_count'] = comm_counts['count'].cumsum()
    comm_counts['date_str'] = comm_counts['First_Production_Date'].dt.strftime('%Y-%m-%d')
    commissioning_data = comm_counts[['date_str', 'cumulative_count']].to_dict('records')

    # --- DATA PREPARATION FOR JSON ---
    site_data = {}
    date_str_map = {d: d.strftime('%Y-%m-%d') for d in date_cols}
    
    for idx, row in df.iterrows():
        site_id = row['Site_ID']
        chart_dates = []
        chart_vals = []
        chart_yields = []
        
        array_size = float(row['Array_Size_kWp']) if pd.notna(row['Array_Size_kWp']) and row['Array_Size_kWp'] > 0 else 1
        
        for d in date_cols:
            val = row[d]
            if pd.notna(val):
                chart_dates.append(date_str_map[d])
                chart_vals.append(round(float(val), 2))
                chart_yields.append(round(float(val) / array_size, 2))

        site_data[site_id] = {
            'name': site_name_map.get(site_id, str(row['Site'])),
            'proj': str(row.get('Project', 'N/A')),
            'grid': str(row.get('Grid Access', 'N/A')),
            'panel': str(row['Panel_Description']),
            'cap': round(float(row['Array_Size_kWp']), 2),
            'prov': row['Province_Full'],
            'yield': round(float(row.get('Avg_Yield_30d_kWh_kWp', 0)), 2),
            'comm': site_commissioned_map.get(site_id, str(row['First_Production_Date'])),
            'd': chart_dates,
            'p': chart_vals,
            'y': chart_yields
        }

    # --- STATS ---
    excellent = df[df['Avg_Yield_30d_kWh_kWp'] > 4.5].to_dict('records')
    good = df[(df['Avg_Yield_30d_kWh_kWp'] >= 3.5) & (df['Avg_Yield_30d_kWh_kWp'] <= 4.5)].to_dict('records')
    fair = df[(df['Avg_Yield_30d_kWh_kWp'] >= 2.5) & (df['Avg_Yield_30d_kWh_kWp'] < 3.5)].to_dict('records')
    poor = df[df['Avg_Yield_30d_kWh_kWp'] < 2.5].to_dict('records')

    prov_stats = df.groupby('Province_Full').agg(
        site_count=('Site_ID', 'count'),
        avg_yield=('Avg_Yield_30d_kWh_kWp', 'mean')
    ).reset_index().sort_values('avg_yield', ascending=False)
    
    proj_stats = df.groupby('Project').agg(
        site_count=('Site_ID', 'count'),
        avg_yield=('Avg_Yield_30d_kWh_kWp', 'mean')
    ).reset_index().sort_values('avg_yield', ascending=False)
    
    panel_stats = df.groupby('Panel_Description').agg(
        site_count=('Site_ID', 'count'),
        avg_yield=('Avg_Yield_30d_kWh_kWp', 'mean')
    ).reset_index().sort_values('avg_yield', ascending=False)

    # --- SAVE DATA TO JSON FILE ---
    full_data_object = {
        'siteData': site_data,
        'degData': degradation_df.to_dict('records') if not degradation_df.empty else [],
        'commData': commissioning_data,
        'gridData': grid_access_stats.to_dict('records'),
        'powerData': power_sources_stats.to_dict('records')
    }
    
    print(f"Saving JSON Data to {OUTPUT_JSON}...", flush=True)
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(full_data_object, f)

    # --- HTML GENERATION (Fetching external JSON) ---
    def gen_list(s, color, cat):
        name = site_name_map.get(s['Site_ID'], s['Site'])
        return f'''<div class="site-list-item" onclick="openSiteModal('{s['Site_ID']}')" style="cursor:pointer; padding:0.5rem; border-left:3px solid {color}; margin-bottom:0.25rem; background:#f8f9fa;">
            <div style="display:flex; justify-content:space-between;"><strong>{name}</strong><span style="color:{color}">{s['Avg_Yield_30d_kWh_kWp']:.2f}</span></div>
            <div style="font-size:0.7em; color:gray;">{s['Panel_Description']}</div></div>'''

    exc_html = ''.join([gen_list(s, '#27ae60', 'excellent') for s in excellent])
    good_html = ''.join([gen_list(s, '#3498db', 'good') for s in good])
    fair_html = ''.join([gen_list(s, '#f39c12', 'fair') for s in fair])
    poor_html = ''.join([gen_list(s, '#e74c3c', 'poor') for s in poor])

    def gen_stat(label, val, sub):
        return f'''<div style="padding:0.5rem; background:white; border-bottom:1px solid #eee;">
            <div style="font-weight:bold; font-size:0.8em;">{label}</div>
            <div style="font-weight:bold;">{val}</div><div style="font-size:0.7em; color:gray;">{sub}</div></div>'''

    prov_html = ''.join([gen_stat(r['Province_Full'], f"{r['avg_yield']:.2f}", f"{r['site_count']} sites") for _, r in prov_stats.iterrows()])
    proj_html = ''.join([gen_stat(r['Project'], f"{r['avg_yield']:.2f}", f"{r['site_count']} sites") for _, r in proj_stats.iterrows()])
    panel_html = ''.join([gen_stat(r['Panel_Description'], f"{r['avg_yield']:.2f}", f"{r['site_count']} sites") for _, r in panel_stats.iterrows()])

    # Minimal HTML that loads the JSON
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Solar Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: sans-serif; background: #f4f6f9; margin: 0; color:#333; }}
        .header {{ background: #2c3e50; color: white; padding: 1rem; }}
        .nav {{ background: white; padding: 0.5rem; display: flex; gap: 1rem; border-bottom: 1px solid #ddd; }}
        .nav-item {{ cursor: pointer; padding: 0.5rem; }}
        .nav-item.active {{ color: #3498db; font-weight: bold; }}
        .container {{ padding: 1rem; max-width: 1400px; margin: 0 auto; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem; }}
        .card {{ background: white; padding: 1rem; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .hidden {{ display: none; }}
        .modal {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 999; }}
        .modal.active {{ display: flex; justify-content: center; align-items: center; }}
        .modal-content {{ background: white; width: 95%; max-width: 1000px; height: 90%; padding: 1rem; overflow-y: auto; }}
        .btn {{ padding: 0.5rem 1rem; cursor: pointer; background: #eee; border: none; margin-right: 0.5rem; }}
        .btn.active {{ background: #3498db; color: white; }}
        #loading {{ padding: 2rem; text-align: center; font-size: 1.2rem; color: #666; }}
    </style>
</head>
<body>
    <div class="header"><h1>🌞 Solar Dashboard</h1><div>{len(df)} Sites | {active_sites} Active</div></div>
    <div class="nav">
        <div class="nav-item active" onclick="showTab('overview')">Overview</div>
        <div class="nav-item" onclick="showTab('sites')">Sites</div>
        <div class="nav-item" onclick="showTab('degradation')">Degradation</div>
        <div class="nav-item" onclick="showTab('performance')">Analysis</div>
    </div>

    <div id="loading">Loading dashboard data...</div>

    <div id="main-content" class="hidden">
        <div id="overview" class="container">
            <div class="grid">
                <div class="card"><h3>Total Capacity</h3><h2>{df['Array_Size_kWp'].sum():,.0f} kWp</h2></div>
                <div class="card"><h3>Avg Yield (30d)</h3><h2>{df['Avg_Yield_30d_kWh_kWp'].mean():.2f}</h2></div>
                <div class="card"><h3>Excellent Sites</h3><h2>{len(excellent)}</h2></div>
            </div>
            <div class="card" style="margin-top:1rem; height:300px;"><canvas id="commChart"></canvas></div>
            <div class="grid" style="margin-top:1rem;">
                <div class="card" style="height:300px;"><canvas id="gridChart"></canvas></div>
                <div class="card" style="height:300px;"><canvas id="powerChart"></canvas></div>
            </div>
        </div>

        <div id="sites" class="container hidden">
            <div class="grid">
                <div class="card"><h3>Excellent</h3><div style="max-height:600px; overflow-y:auto">{exc_html}</div></div>
                <div class="card"><h3>Good</h3><div style="max-height:600px; overflow-y:auto">{good_html}</div></div>
                <div class="card"><h3>Fair</h3><div style="max-height:600px; overflow-y:auto">{fair_html}</div></div>
                <div class="card"><h3>Poor</h3><div style="max-height:600px; overflow-y:auto">{poor_html}</div></div>
            </div>
        </div>

        <div id="degradation" class="container hidden">
            <div class="card"><h3>Degradation Analysis</h3><div id="deg-list"></div></div>
        </div>

        <div id="performance" class="container hidden">
            <div class="grid"><div class="card"><h3>Province</h3>{prov_html}</div><div class="card"><h3>Project</h3>{proj_html}</div><div class="card"><h3>Panel</h3>{panel_html}</div></div>
        </div>
    </div>

    <div id="site-modal" class="modal"><div class="modal-content">
        <div style="display:flex; justify-content:space-between;"><h2>Site Details</h2><button onclick="document.getElementById('site-modal').classList.remove('active')">Close</button></div>
        <div style="margin-bottom:1rem;">
            <button class="btn" onclick="loadSiteData('7d')">7 Days</button>
            <button class="btn" onclick="loadSiteData('30d')">30 Days</button>
            <button class="btn active" onclick="loadSiteData('90d')">90 Days</button>
            <button class="btn" onclick="loadSiteData('all')">All</button>
        </div>
        <div id="modal-info" style="display:grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap:1rem; margin-bottom:1rem; background:#f8f9fa; padding:1rem;"></div>
        <div style="height:300px; margin-bottom:1rem;"><canvas id="dailyChart"></canvas></div>
        <div style="height:300px;"><canvas id="yieldChart"></canvas></div>
    </div></div>

    <script>
        let siteData = {{}};
        let currentId = null;
        let charts = [];

        // FETCH DATA ON LOAD
        fetch('dashboard_data.json')
            .then(response => response.json())
            .then(data => {{
                siteData = data.siteData;
                initDashboard(data);
                document.getElementById('loading').style.display = 'none';
                document.getElementById('main-content').classList.remove('hidden');
            }})
            .catch(err => {{
                console.error("Error loading data:", err);
                document.getElementById('loading').innerText = "Error loading data.";
            }});

        function initDashboard(data) {{
            // Charts
            new Chart(document.getElementById('commChart'), {{type:'line', data:{{labels:data.commData.map(d=>d.date_str), datasets:[{{label:'Sites', data:data.commData.map(d=>d.cumulative_count), borderColor:'#3498db', fill:true}}]}}, options:{{maintainAspectRatio:false}} }});
            new Chart(document.getElementById('gridChart'), {{type:'pie', data:{{labels:data.gridData.map(d=>d['Grid Access']), datasets:[{{data:data.gridData.map(d=>d.site_count), backgroundColor:['#3498db','#27ae60','#f39c12','#e74c3c']}}]}}, options:{{maintainAspectRatio:false}} }});
            new Chart(document.getElementById('powerChart'), {{type:'pie', data:{{labels:data.powerData.map(d=>d['Power Sources']), datasets:[{{data:data.powerData.map(d=>d.site_count), backgroundColor:['#e74c3c','#3498db','#f39c12','#9b59b6']}}]}}, options:{{maintainAspectRatio:false}} }});

            // Deg List
            const degList = document.getElementById('deg-list');
            const degData = data.degData.sort((a,b) => b.actual_degradation - a.actual_degradation);
            let dHtml = '';
            degData.forEach(s => {{
                let c = s.actual_degradation > 30 ? '#e74c3c' : '#27ae60';
                dHtml += `<div style="padding:0.5rem; border-left:4px solid ${{c}}; background:#f8f9fa; margin-bottom:0.5rem;"><b>${{s.site_name}}</b>: ${{s.actual_degradation}}% (Exp: ${{s.expected_degradation}}%)</div>`;
            }});
            degList.innerHTML = dHtml || 'No data';
        }}

        function showTab(id) {{
            document.querySelectorAll('.container').forEach(el => el.classList.add('hidden'));
            document.getElementById(id).classList.remove('hidden');
            document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
            event.target.classList.add('active');
        }}

        window.openSiteModal = function(id) {{
            currentId = id;
            const s = siteData[id];
            if(!s) return;
            document.getElementById('site-modal').classList.add('active');
            document.getElementById('modal-info').innerHTML = `<div><b>${{s.name}}</b></div><div>${{s.prov}}</div><div>${{s.cap}} kWp</div><div>${{s.panel}}</div>`;
            loadSiteData('90d');
        }}

        window.loadSiteData = function(period) {{
            const s = siteData[currentId];
            if(!s) return;
            
            let dates = s.d;
            let prod = s.p;
            let yld = s.y;
            
            if(period !== 'all') {{
                const limit = {{'7d':7, '30d':30, '90d':90}}[period];
                const startIdx = Math.max(0, dates.length - limit);
                dates = dates.slice(startIdx);
                prod = prod.slice(startIdx);
                yld = yld.slice(startIdx);
            }}

            charts.forEach(c => c.destroy());
            charts = [];
            
            charts.push(new Chart(document.getElementById('dailyChart'), {{type:'bar', data:{{labels:dates, datasets:[{{label:'kWh', data:prod, backgroundColor:'#3498db'}}]}}, options:{{maintainAspectRatio:false}} }}));
            charts.push(new Chart(document.getElementById('yieldChart'), {{type:'line', data:{{labels:dates, datasets:[{{label:'Yield', data:yld, borderColor:'#27ae60'}}]}}, options:{{maintainAspectRatio:false}} }}));
        }}
    </script>
</body>
</html>"""
    
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    if os.path.exists('temp_master.parquet'):
        os.remove('temp_master.parquet')
    
    print(f"Dashboard generated: {OUTPUT_HTML} and {OUTPUT_JSON}", flush=True)

def main():
    service = get_drive_service()
    pivot_df = sync_and_load_data(service)
    
    if not pivot_df.empty:
        final_df, date_cols = process_data(pivot_df)
        generate_html(final_df, date_cols)
    else:
        print("No data found to process.", flush=True)

if __name__ == "__main__":
    main()
