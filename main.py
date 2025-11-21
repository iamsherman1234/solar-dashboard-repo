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
DRIVE_FOLDER_ID = '1jhw0IRHwG8ogRCL9g9Qu3RAsN0gkNLPI' 

# Paths (Adjusted for ROOT directory)
# We removed the "../" because data folder is now next to main.py
METADATA_FILE = os.path.join(os.path.dirname(__file__), 'data/sites_metadata.xlsx')
ADDITIONAL_INFO = os.path.join(os.path.dirname(__file__), 'data/additional_site_info.csv')
OUTPUT_HTML = 'index.html'

# Province Mapping
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
        creds_dict, scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)

def download_monitoring_data(service):
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
            request = service.files().get_media(fileId=file['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            # --- ROBUST HEADER DETECTION PATCH ---
            header_row_index = 0
            fh.seek(0)
            try:
                # Scan first 50 rows
                df_test = pd.read_excel(fh, header=None, nrows=50, engine='openpyxl')
                found_header = False
                for i, row in df_test.iterrows():
                    row_values = [str(val).strip() for val in row.values]
                    if 'Site' in row_values and 'Solar Supply (kWh)' in row_values:
                        header_row_index = i
                        found_header = True
                        break
                if not found_header:
                    print(f"    ⚠ Skipped {file['name']} (Header not found)")
                    continue
            except: continue

            # Read Actual Data
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
                all_data.append(temp_df)
        except Exception as e:
            print(f"Skipping {file['name']}: {e}")

    if not all_data:
        return pd.DataFrame()
        
    combined = pd.concat(all_data, ignore_index=True)
    combined = combined.sort_values('Date').drop_duplicates(subset=['Site_ID', 'Date'], keep='last')
    pivot_df = combined.pivot(index='Site_ID', columns='Date', values='Solar_kWh').reset_index()
    return pivot_df

def process_data(pivot_df):
    print("Loading Metadata and Calculating Stats...")
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
    print("Generating HTML...")

    # Load DB Info
    site_name_map = {}
    if os.path.exists(ADDITIONAL_INFO):
        try:
            db_df = pd.read_csv(ADDITIONAL_INFO)
            if 'site_id' in db_df.columns:
                site_name_map = dict(zip(db_df['site_id'], db_df.get('site_name', db_df['site_id'])))
        except: pass

    def get_province(site_id):
        if isinstance(site_id, str) and len(site_id) >= 2: return PROVINCE_MAPPING.get(site_id[:2].upper(), site_id[:2])
        return 'Unknown'
    df['Province_Full'] = df['Site_ID'].apply(get_province)

    # --- DEGRADATION ANALYSIS ---
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
                
                degradation_data.append({
                    'site_id': site_id,
                    'site_name': site_name_map.get(site_id, row['Site']),
                    'actual_degradation': actual_deg,
                    'expected_degradation': expected,
                    'years_elapsed': years_elapsed
                })
    
    degradation_df = pd.DataFrame(degradation_data)

    # --- JSON DATA PREP ---
    site_data = {}
    for idx, row in df.iterrows():
        site_id = row['Site_ID']
        daily = []
        for d in date_cols:
            if pd.notna(row[d]):
                daily.append({
                    'date': d.strftime('%Y-%m-%d'),
                    'specific_yield': float(row[d]) / row['Array_Size_kWp'] if row['Array_Size_kWp'] > 0 else 0
                })
        
        site_data[site_id] = {
            'site_id': site_id,
            'site_name': site_name_map.get(site_id, str(row['Site'])),
            'project': str(row.get('Project', 'N/A')),
            'panel_description': str(row['Panel_Description']),
            'array_size_kwp': float(row['Array_Size_kWp']),
            'province': row['Province_Full'],
            'avg_yield_30d': float(row.get('Avg_Yield_30d_kWh_kWp', 0)),
            'daily_data': daily
        }

    # Groups
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

    # Helper HTML
    def gen_list(s, color, cat):
        name = site_name_map.get(s['Site_ID'], s['Site'])
        return f'''<div class="site-list-item" onclick="openSiteModal('{s['Site_ID']}')" style="cursor:pointer; padding:0.75rem; border-left:3px solid {color}; margin-bottom:0.5rem; background:#f8f9fa; border-radius:0.5rem;">
            <div style="display:flex; justify-content:space-between;"><strong>{name}</strong><span style="color:{color}">{s['Avg_Yield_30d_kWh_kWp']:.2f} kWh/kWp</span></div>
            <div style="font-size:0.8em; color:gray;">{s['Panel_Description']}</div></div>'''

    exc_html = ''.join([gen_list(s, '#27ae60', 'excellent') for s in excellent])
    good_html = ''.join([gen_list(s, '#3498db', 'good') for s in good])
    fair_html = ''.join([gen_list(s, '#f39c12', 'fair') for s in fair])
    poor_html = ''.join([gen_list(s, '#e74c3c', 'poor') for s in poor])
    prov_html = ''.join([f'<div class="card" style="margin-bottom:0.5rem; padding:0.5rem;"><strong>{r["Province_Full"]}</strong>: {r["avg_yield"]:.2f}</div>' for _, r in prov_stats.iterrows()])

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Solar Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: -apple-system, system-ui, sans-serif; background: #f0f2f5; margin: 0; }}
        .header {{ background: #2c3e50; color: white; padding: 1rem 2rem; display: flex; justify-content: space-between; }}
        .nav {{ background: white; padding: 0 2rem; border-bottom: 1px solid #ddd; display: flex; gap: 2rem; }}
        .nav-item {{ padding: 1rem 0.5rem; cursor: pointer; border-bottom: 3px solid transparent; }}
        .nav-item.active {{ border-bottom-color: #3498db; color: #3498db; font-weight: bold; }}
        .container {{ padding: 2rem; max-width: 1400px; margin: 0 auto; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1.5rem; }}
        .card {{ background: white; padding: 1.5rem; border-radius: 0.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .stat-val {{ font-size: 2rem; font-weight: bold; margin: 0.5rem 0; }}
        .hidden {{ display: none; }}
        .modal {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 1000; align-items: center; justify-content: center; }}
        .modal.active {{ display: flex; }}
        .modal-content {{ background: white; width: 90%; max-width: 1000px; height: 90%; border-radius: 1rem; padding: 2rem; overflow-y: auto; }}
    </style>
</head>
<body>
    <div class="header"><h1>🌞 Solar Dashboard</h1><div>{len(df)} Sites</div></div>
    <div class="nav">
        <div class="nav-item active" onclick="showTab('overview')">Overview</div>
        <div class="nav-item" onclick="showTab('sites')">Site Lists</div>
        <div class="nav-item" onclick="showTab('degradation')">Degradation</div>
    </div>

    <div class="container" id="overview">
        <div class="grid">
            <div class="card" style="border-top:4px solid #27ae60"><div>Avg Yield</div><div class="stat-val">{df['Avg_Yield_30d_kWh_kWp'].mean():.2f}</div></div>
            <div class="card" style="border-top:4px solid #e74c3c"><div>Critical Alerts</div><div class="stat-val">{len(crit_alerts)}</div></div>
            <div class="card"><h3>Province Stats</h3>{prov_html}</div>
        </div>
    </div>

    <div class="container hidden" id="sites">
        <div class="grid">
            <div class="card"><h3>🌟 Excellent</h3>{exc_html}</div>
            <div class="card"><h3>✅ Good</h3>{good_html}</div>
            <div class="card"><h3>⚠️ Fair</h3>{fair_html}</div>
            <div class="card"><h3>🚨 Poor</h3>{poor_html}</div>
        </div>
    </div>

    <div class="container hidden" id="degradation">
        <div class="card"><h3>Degradation Analysis</h3><div id="deg-list"></div></div>
    </div>

    <div id="site-modal" class="modal">
        <div class="modal-content">
            <div style="display:flex; justify-content:space-between;">
                <h2 id="modal-title">Site</h2>
                <button onclick="document.getElementById('site-modal').classList.remove('active')">Close</button>
            </div>
            <div id="modal-info" style="margin:1rem 0; padding:1rem; background:#f8f9fa;"></div>
            <canvas id="siteChart"></canvas>
        </div>
    </div>

    <script>
        const siteData = {json.dumps(site_data)};
        const degData = {json.dumps(degradation_df.to_dict('records') if not degradation_df.empty else [])};

        function showTab(id) {{
            document.querySelectorAll('.container').forEach(d => d.classList.add('hidden'));
            document.getElementById(id).classList.remove('hidden');
            document.querySelectorAll('.nav-item').forEach(d => d.classList.remove('active'));
            event.target.classList.add('active');
        }}

        // Degradation List
        const degList = document.getElementById('deg-list');
        degData.sort((a,b) => b.actual_degradation - a.actual_degradation);
        let degHtml = '';
        degData.forEach(s => {{
            let color = s.actual_degradation > 30 ? '#e74c3c' : '#27ae60';
            degHtml += `<div style="padding:1rem; border-left:4px solid ${{color}}; background:#f8f9fa; margin-bottom:0.5rem;">
                <strong>${{s.site_name}}</strong>: ${{s.actual_degradation.toFixed(1)}}% (Exp: ${{s.expected_degradation.toFixed(1)}}%)
            </div>`;
        }});
        degList.innerHTML = degHtml || 'No data.';

        // Modal
        let chartInstance = null;
        window.openSiteModal = function(id) {{
            const s = siteData[id];
            if(!s) return;
            document.getElementById('site-modal').classList.add('active');
            document.getElementById('modal-title').innerText = s.site_name;
            document.getElementById('modal-info').innerHTML = `Province: ${{s.province}} | Capacity: ${{s.array_size_kwp}} kWp`;
            
            const ctx = document.getElementById('siteChart').getContext('2d');
            if(chartInstance) chartInstance.destroy();
            chartInstance = new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: s.daily_data.map(d => d.date),
                    datasets: [{{
                        label: 'Specific Yield',
                        data: s.daily_data.map(d => d.specific_yield),
                        borderColor: '#3498db',
                        fill: false
                    }}]
                }}
            }});
        }};
    </script>
</body>
</html>"""

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Dashboard generated at {OUTPUT_HTML}")

def main():
    service = get_drive_service()
    pivot_df = download_monitoring_data(service)
    if not pivot_df.empty:
        final_df, date_cols = process_data(pivot_df)
        generate_html(final_df, date_cols)
    else:
        print("No data found.")

if __name__ == "__main__":
    main()
