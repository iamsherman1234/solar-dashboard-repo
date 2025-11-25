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
    """
    Generates a single self-contained HTML dashboard with embedded JSON data.
    """
    print("\n[4/5] Generating Dashboard HTML...")

    # 1. PREPARE STATISTICS & GROUPING
    # ---------------------------------------------------------
    
    # Fleet Totals
    total_sites = len(df)
    sites_with_data = len(df[df['Days_With_Data'] > 0])
    total_capacity = df['Array_Size_kWp'].sum()
    
    # Yield Averages (Weighted)
    df_calc = df.fillna(0)
    if total_capacity > 0:
        avg_yield_30d = (df_calc['Avg_Yield_30d_kWh_kWp'] * df_calc['Array_Size_kWp']).sum() / total_capacity
        avg_yield_7d = (df_calc.get('Avg_Yield_7d_kWh_kWp', 0) * df_calc['Array_Size_kWp']).sum() / total_capacity
        avg_yield_90d = (df_calc.get('Avg_Yield_90d_kWh_kWp', 0) * df_calc['Array_Size_kWp']).sum() / total_capacity
    else:
        avg_yield_30d = avg_yield_7d = avg_yield_90d = 0

    # Critical Alerts (Offline > 3 days)
    date_cols_sorted = sorted(date_cols)
    last_3_days = date_cols_sorted[-3:] if len(date_cols_sorted) >= 3 else date_cols_sorted
    
    critical_alerts = []
    for _, row in df.iterrows():
        if row['Days_With_Data'] > 0:
             if all(pd.isna(row[d]) or row[d] == 0 for d in last_3_days):
                critical_alerts.append(row['Site_ID'])

    # Categories (Excellent, Good, etc.)
    excellent_sites = df[df['Avg_Yield_30d_kWh_kWp'] > 4.5].fillna(0).to_dict('records')
    good_sites = df[(df['Avg_Yield_30d_kWh_kWp'] > 3.5) & (df['Avg_Yield_30d_kWh_kWp'] <= 4.5)].fillna(0).to_dict('records')
    fair_sites = df[(df['Avg_Yield_30d_kWh_kWp'] > 2.5) & (df['Avg_Yield_30d_kWh_kWp'] <= 3.5)].fillna(0).to_dict('records')
    poor_sites = df[df['Avg_Yield_30d_kWh_kWp'] <= 2.5].fillna(0).to_dict('records')

    # Grouping Helper
    def get_weighted_stats(group_col, label):
        valid = df[df['Array_Size_kWp'] > 0].copy()
        if valid.empty: return pd.DataFrame()
        stats = valid.groupby(group_col).agg(
            site_count=('Site_ID', 'count'),
            total_capacity=('Array_Size_kWp', 'sum'),
            avg_yield=('Avg_Yield_30d_kWh_kWp', lambda x: (x * valid.loc[x.index, 'Array_Size_kWp']).sum() / valid.loc[x.index, 'Array_Size_kWp'].sum())
        ).reset_index().rename(columns={group_col: label}).sort_values('avg_yield', ascending=False)
        return stats

    # Generate Stats Tables
    province_stats = get_weighted_stats('Province_Full', 'province')
    project_stats = get_weighted_stats('Project', 'project')
    panel_stats = get_weighted_stats('Panel_Description', 'panel_type')
    grid_access_stats = df.groupby('Grid Access').agg(site_count=('Site_ID', 'count')).reset_index()
    power_sources_stats = df.groupby('Power Sources').agg(site_count=('Site_ID', 'count')).reset_index()

    # Timeline Data
    commissioning_timeline = df[df['First_Production_Date'].notna()].copy()
    commissioning_timeline['First_Production_Date'] = pd.to_datetime(commissioning_timeline['First_Production_Date'])
    date_counts = commissioning_timeline.sort_values('First_Production_Date').groupby('First_Production_Date').size().reset_index(name='count')
    date_counts['cumulative_count'] = date_counts['count'].cumsum()
    date_counts['First_Production_Date'] = date_counts['First_Production_Date'].dt.strftime('%Y-%m-%d')
    
    # 2. PREPARE SITE DETAIL DATA (The Big Object)
    # ---------------------------------------------------------
    site_data = {}
    for idx, row in df.iterrows():
        site_id = row['Site_ID']
        daily_data = []
        for d in date_cols:
            if pd.notna(row[d]):
                daily_data.append({
                    'date': d, 
                    'solar_supply_kwh': float(row[d]),
                    'specific_yield': float(row[d])/float(row['Array_Size_kWp']) if pd.notna(row['Array_Size_kWp']) and row['Array_Size_kWp'] > 0 else 0
                })
        
        # Helper for safe type conversion for JSON serialization
        def safe(val, typ=str): 
            if pd.isna(val): return 'N/A' if typ==str else 0
            return typ(val)

        site_data[site_id] = {
            'site_id': site_id,
            'site_name': str(row.get('Site', site_id)),
            'project': safe(row.get('Project')),
            'panel_description': safe(row.get('Panel_Description')),
            'array_size_kwp': safe(row.get('Array_Size_kWp'), float),
            'province': safe(row.get('Province_Full')),
            'commissioned_date': str(row.get('First_Production_Date')),
            'daily_data': daily_data,
            'prod_30d': safe(row.get('Prod_30d_kWh'), float),
            'avg_yield_30d': safe(row.get('Avg_Yield_30d_kWh_kWp'), float),
            'grid_access': safe(row.get('Grid Access')),
            'power_sources': safe(row.get('Power Sources')),
            'po': safe(row.get('PO')),
            'panels': safe(row.get('Panels'), int),
            'panel_size': safe(row.get('Panel Size'), int),
            'panel_vendor': safe(row.get('Panel Vendor')),
            'panel_model': safe(row.get('Panel Model')),
        }

    # 3. DEGRADATION CALCULATION
    # ---------------------------------------------------------
    print("  Calculating degradation metrics...")
    degradation_list = []
    # Re-create date index for speed
    date_dt_map = {c: pd.to_datetime(c) for c in date_cols}
    latest_dt = pd.to_datetime(date_cols_sorted[-1])
    
    for idx, row in df.iterrows():
        if pd.isna(row['Array_Size_kWp']) or row['Array_Size_kWp'] == 0: continue
        if pd.isna(row['First_Production_Date']): continue
        
        try: first_dt = pd.to_datetime(str(row['First_Production_Date']))
        except: continue
        
        comm_end = first_dt + timedelta(days=30)
        last_start = latest_dt - timedelta(days=30)
        
        # Fast filtering
        c_vals = [row[c] for c in date_cols if first_dt <= date_dt_map[c] <= comm_end and pd.notna(row[c]) and row[c]>0]
        l_vals = [row[c] for c in date_cols if last_start <= date_dt_map[c] <= latest_dt and pd.notna(row[c]) and row[c]>0]
        
        if len(c_vals) > 5 and len(l_vals) > 5:
            arr = row['Array_Size_kWp']
            init_95 = np.percentile(c_vals, 95) / arr
            curr_95 = np.percentile(l_vals, 95) / arr
            years = (latest_dt - first_dt).days / 365.25
            
            exp = years * 1.5 if years <= 1 else 1.5 + (years-1)*0.4
            act = (init_95 - curr_95)/init_95 * 100 if init_95 > 0 else 0
            
            recent_cols = date_cols_sorted[-3:]
            has_recent = any(pd.notna(row[c]) and row[c]>0 for c in recent_cols)
            
            degradation_list.append({
                'site_id': row['Site_ID'],
                'site_name': str(row.get('Site', row['Site_ID'])),
                'actual_degradation': act,
                'expected_degradation': exp,
                'performance_vs_expected': exp - act,
                'years_elapsed': years,
                'has_recent_data': has_recent,
                'panel_description': str(row.get('Panel_Description', 'N/A')),
                'array_size': arr
            })
    
    degradation_df = pd.DataFrame(degradation_list)
    print(f"  ✓ Degradation calculated for {len(degradation_df)} sites.")

    # 4. SERIALIZE DATA TO JSON STRINGS
    # ---------------------------------------------------------
    # This replaces writing to a file. We convert everything to strings here.
    
    json_site_data = json.dumps(site_data)
    json_all_ids = json.dumps([str(s) for s in df['Site_ID']])
    json_degradation = json.dumps(degradation_df.to_dict('records') if not degradation_df.empty else [])
    
    json_province = json.dumps(province_stats.to_dict('records'))
    json_project = json.dumps(project_stats.to_dict('records'))
    json_panel = json.dumps(panel_stats.to_dict('records'))
    json_comm = json.dumps(date_counts.to_dict('records'))
    json_grid = json.dumps(grid_access_stats.to_dict('records'))
    json_power = json.dumps(power_sources_stats.to_dict('records'))
    
    # Site Lists for JS
    json_exc = json.dumps([str(s['Site_ID']) for s in excellent_sites])
    json_good = json.dumps([str(s['Site_ID']) for s in good_sites])
    json_fair = json.dumps([str(s['Site_ID']) for s in fair_sites])
    json_poor = json.dumps([str(s['Site_ID']) for s in poor_sites])

    # 5. GENERATE HTML STRINGS
    # ---------------------------------------------------------
    def make_list_html(sites, color):
        h = ""
        for s in sites:
            name = str(s.get('Site', s.get('Site_ID')))
            yield_v = s.get('Avg_Yield_30d_kWh_kWp', 0)
            desc = s.get('Panel_Description', '')
            size = s.get('Array_Size_kWp', 0)
            h += f'''<div class="site-list-item" onclick="openSiteModal('{s['Site_ID']}', 'all')" style="cursor:pointer; padding:0.75rem; border-left:3px solid {color}; margin-bottom:0.5rem; background:#f8f9fa; border-radius:0.5rem;">
            <div style="display:flex; justify-content:space-between;"><div style="font-weight:600;">{name}</div><div style="font-weight:bold;">{yield_val:.2f}</div></div>
            <div style="font-size:0.8em; color:#666;">{desc} • {size:.1f} kWp</div></div>'''
        return h if h else '<p style="padding:1rem; color:#666;">No sites in this category</p>'

    html_exc = make_list_html(excellent_sites, '#27ae60')
    html_good = make_list_html(good_sites, '#3498db')
    html_fair = make_list_html(fair_sites, '#f39c12')
    html_poor = make_list_html(poor_sites, '#e74c3c')

    def make_card_html(stats, label_key):
        h = ""
        for _, r in stats.iterrows():
            v = r['avg_yield']
            c = '#27ae60' if v > 4.0 else ('#f39c12' if v > 3.0 else '#e74c3c')
            h += f'''<div class="stat-card" style="border-left:4px solid {c}; background:white; padding:1rem; border-radius:0.5rem; box-shadow:0 1px 3px rgba(0,0,0,0.1);">
            <div style="font-size:0.8rem; font-weight:bold; color:#666;">{r[label_key]}</div><div style="font-size:1.5rem; font-weight:bold;">{v:.2f}</div>
            <div style="font-size:0.8rem; color:#666;">{int(r['site_count'])} sites • {r['total_capacity']:.0f} kWp</div></div>'''
        return h

    html_prov = make_card_html(province_stats, 'province')
    html_proj = make_card_html(project_stats, 'project')
    html_panel = make_card_html(panel_stats, 'panel_type')

    # 6. ASSEMBLE FINAL HTML
    # ---------------------------------------------------------
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Solar Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
    <style>
        * {{ margin:0; padding:0; box-sizing:border-box; }}
        body {{ font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background:#f4f6f9; color:#333; }}
        body.dark-mode {{ background:#1a1a1a; color:#e0e0e0; }}
        .header {{ background:linear-gradient(135deg, #3498db, #2980b9); color:white; padding:1.5rem 2rem; display:flex; justify-content:space-between; align-items:center; box-shadow:0 4px 6px rgba(0,0,0,0.1); }}
        .dark-mode .header {{ background:linear-gradient(135deg, #2c3e50, #34495e); }}
        .theme-toggle {{ background:rgba(255,255,255,0.2); border:1px solid rgba(255,255,255,0.3); padding:0.5rem 1rem; color:white; border-radius:0.5rem; cursor:pointer; }}
        .nav {{ background:#e9ecef; padding:0 2rem; display:flex; gap:2rem; border-bottom:1px solid #dee2e6; }}
        .dark-mode .nav {{ background:#2d3748; border-bottom:1px solid #4a5568; }}
        .nav-item {{ padding:1rem 0.5rem; cursor:pointer; border-bottom:3px solid transparent; font-weight:500; color:#6c757d; }}
        .nav-item.active {{ color:#3498db; border-bottom-color:#3498db; }}
        .dark-mode .nav-item {{ color:#a0aec0; }}
        .dark-mode .nav-item.active {{ color:#4299e1; border-bottom-color:#4299e1; }}
        .container {{ max-width:1400px; margin:0 auto; padding:2rem; }}
        .stats-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(250px, 1fr)); gap:1.5rem; margin-bottom:2rem; }}
        .stat-card {{ background:white; border-radius:0.75rem; padding:1.5rem; box-shadow:0 1px 3px rgba(0,0,0,0.1); border-left:4px solid #3498db; }}
        .dark-mode .stat-card {{ background:#2d3748; }}
        .chart-container {{ background:white; border-radius:0.75rem; padding:1.5rem; margin-bottom:1.5rem; box-shadow:0 1px 3px rgba(0,0,0,0.1); position:relative; }}
        .dark-mode .chart-container {{ background:#2d3748; }}
        .dark-mode h3, .dark-mode h4 {{ color:#e2e8f0; }}
        .hidden {{ display:none; }}
        .scroll-list {{ max-height:400px; overflow-y:auto; }}
        .export-btn {{ position:absolute; top:1rem; right:1rem; background:#27ae60; color:white; border:none; padding:0.5rem 1rem; border-radius:0.3rem; cursor:pointer; font-size:0.85rem; display:flex; align-items:center; gap:0.5rem; transition:0.2s; }}
        .export-btn:hover {{ background:#219150; }}
        .modal-overlay {{ display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.7); z-index:1000; align-items:center; justify-content:center; }}
        .modal-overlay.active {{ display:flex; }}
        .modal-content {{ background:white; width:90%; max-width:1200px; height:90%; border-radius:0.75rem; overflow:hidden; display:flex; flex-direction:column; }}
        .dark-mode .modal-content {{ background:#2d3748; }}
        .modal-header {{ background:linear-gradient(135deg, #3498db, #2980b9); color:white; padding:1rem 1.5rem; display:flex; justify-content:space-between; align-items:center; }}
        .dark-mode .modal-header {{ background:linear-gradient(135deg, #2c3e50, #34495e); }}
        .modal-body {{ padding:1.5rem; overflow-y:auto; flex:1; }}
        .site-info-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:0.75rem; margin-bottom:1rem; }}
        .site-info-item {{ background:#f8f9fa; padding:0.75rem; border-left:3px solid #3498db; border-radius:0.5rem; }}
        .dark-mode .site-info-item {{ background:#1a365d; border-left-color:#2b6cb0; }}
        .site-info-label {{ font-size:0.75rem; color:#6c757d; font-weight:600; }}
        .dark-mode .site-info-label {{ color:#a0aec0; }}
        .site-info-value {{ font-size:1.125rem; font-weight:600; color:#333; }}
        .dark-mode .site-info-value {{ color:#e2e8f0; }}
        .time-period-selector {{ display:flex; gap:0.5rem; margin-bottom:1rem; background:#e9ecef; padding:0.375rem; border-radius:0.5rem; }}
        .dark-mode .time-period-selector {{ background:#1a365d; }}
        .period-button {{ flex:1; padding:0.5rem; border:none; background:white; cursor:pointer; border-radius:0.3rem; transition:0.2s; }}
        .period-button.active {{ background:linear-gradient(135deg, #3498db, #2980b9); color:white; }}
        .dark-mode .period-button {{ background:#2d3748; color:#a0aec0; }}
        .dark-mode .period-button.active {{ background:linear-gradient(135deg, #2b6cb0, #2c5282); }}
        .chart-wrapper {{ background:white; padding:1rem; border-radius:0.5rem; box-shadow:0 1px 3px rgba(0,0,0,0.1); }}
        .dark-mode .chart-wrapper {{ background:#1a365d; }}
        .stats-summary {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(140px, 1fr)); gap:0.75rem; margin-top:1rem; }}
        .summary-card {{ background:linear-gradient(135deg, #3498db, #2980b9); color:white; padding:1rem; border-radius:0.5rem; text-align:center; }}
        .summary-card.green {{ background:linear-gradient(135deg, #27ae60, #229954); }}
        .summary-card.yellow {{ background:linear-gradient(135deg, #f39c12, #e67e22); }}
        .summary-card.red {{ background:linear-gradient(135deg, #e74c3c, #c0392b); }}
        .summary-label {{ font-size:0.75rem; opacity:0.9; margin-bottom:0.25rem; }}
        .summary-value {{ font-size:1.5rem; font-weight:700; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="header-content"><h1>Solar Performance Dashboard</h1><p>Data from {total_sites} sites • Capacity: {total_capacity:.1f} kWp</p></div>
        <button class="theme-toggle" onclick="toggleTheme()">🌙 Dark Mode</button>
    </div>

    <div class="nav">
        <div class="nav-item active" onclick="showTab(this, 'overview')">Overview</div>
        <div class="nav-item" onclick="showTab(this, 'sites')">All Sites</div>
        <div class="nav-item" onclick="showTab(this, 'degradation')">Degradation Analysis</div>
        <div class="nav-item" onclick="showTab(this, 'performance')">Performance Categories</div>
    </div>

    <div class="container">
        <div id="overview-tab">
            <button class="export-btn" onclick="exportOverview()">📥 Export Overview</button>
            <div class="stats-grid" style="margin-top:3rem;">
                <div class="stat-card blue"><div class="stat-label">Total Sites</div><div class="stat-value">{total_sites}</div><div class="stat-subtitle">{sites_with_data} online</div></div>
                <div class="stat-card green"><div class="stat-label">Total Capacity</div><div class="stat-value">{total_capacity:.1f}</div><div class="stat-subtitle">kWp installed capacity</div></div>
                <div class="stat-card yellow"><div class="stat-label">Avg Specific Yield</div><div class="stat-value">{avg_yield_30d:.2f}</div><div class="stat-subtitle">kWh/kWp/day (30-day avg)</div></div>
                <div class="stat-card red"><div class="stat-label">Critical Alerts</div><div class="stat-value">{len(critical_alerts)}</div><div class="stat-subtitle">Sites with 0 production (last 3 days)</div></div>
            </div>
            
            <div class="chart-container">
                <h3>Performance & Health</h3>
                <div style="display:grid; grid-template-columns:1fr 1fr; gap:2rem; margin-top:1rem;">
                    <div><canvas id="distChart"></canvas></div>
                    <div>
                        <h4 style="margin-bottom:1rem;">Fleet Health Metrics</h4>
                        <div style="padding:1rem; background:#f8f9fa; border-radius:0.5rem;">
                            <p><strong>Last 7 Days:</strong> {avg_yield_7d:.2f} kWh/kWp/day</p>
                            <p><strong>Last 30 Days:</strong> {avg_yield_30d:.2f} kWh/kWp/day</p>
                            <p><strong>Last 90 Days:</strong> {avg_yield_90d:.2f} kWh/kWp/day</p>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="chart-container">
                <h3>Fleet Composition</h3>
                <div style="display:grid; grid-template-columns:1fr 1fr; gap:2rem; margin-top:1rem;">
                    <div><h4 style="margin-bottom:1rem;">Grid Access</h4><canvas id="gridAccessChart" style="max-height:300px;"></canvas></div>
                    <div><h4 style="margin-bottom:1rem;">Power Sources</h4><canvas id="powerSourcesChart" style="max-height:300px;"></canvas></div>
                </div>
            </div>
            <div class="chart-container"><h3>Commissioning Timeline</h3><canvas id="commissioningChart" style="margin-top:1rem;"></canvas></div>
        </div>

        <div id="sites-tab" class="hidden">
            <button class="export-btn" onclick="exportAllSites()">📥 Export Master List</button>
            <div style="margin-top:3rem;">
                <div class="chart-container"><h3>🌟 Excellent Performance (>4.5 kWh/kWp/day) - {len(excellent_sites)} sites</h3><div class="scroll-list">{html_exc}</div></div>
                <div class="chart-container"><h3>✅ Good Performance (3.5-4.5 kWh/kWp/day) - {len(good_sites)} sites</h3><div class="scroll-list">{html_good}</div></div>
                <div class="chart-container"><h3>⚠️ Fair Performance (2.5-3.5 kWh/kWp/day) - {len(fair_sites)} sites</h3><div class="scroll-list">{html_fair}</div></div>
                <div class="chart-container"><h3>🚨 Poor Performance (<2.5 kWh/kWp/day) - {len(poor_sites)} sites</h3><div class="scroll-list">{html_poor}</div></div>
            </div>
        </div>

        <div id="degradation-tab" class="hidden">
            <button class="export-btn" onclick="exportDegradation()">📥 Export Degradation Report</button>
            <div style="margin-top:3rem;">
                <div class="chart-container"><h3>🚨 Offline / No Data</h3><div id="offline-sites-list" class="scroll-list"></div></div>
                <div class="chart-container"><h3>🔴 High Degradation (>50%)</h3><div id="high-degradation-list" class="scroll-list"></div></div>
                <div class="chart-container"><h3>⚠️ Medium Degradation (30-50%)</h3><div id="medium-degradation-list" class="scroll-list"></div></div>
                <div class="chart-container"><h3>✅ Low Degradation (0-30%)</h3><div id="low-degradation-list" class="scroll-list"></div></div>
                <div class="chart-container"><h3>🌟 Better Than Expected</h3><div id="better-degradation-list" class="scroll-list"></div></div>
            </div>
        </div>

        <div id="performance-tab" class="hidden">
            <button class="export-btn" onclick="exportPerformanceStats()">📥 Export Performance Stats</button>
            <div style="margin-top:3rem;">
                <div class="chart-container"><h3>🏢 Performance by Province</h3><div class="stats-grid">{html_prov}</div></div>
                <div class="chart-container"><h3>📋 Performance by Project</h3><div class="stats-grid">{html_proj}</div></div>
                <div class="chart-container"><h3>⚡ Performance by Panel Type</h3><div class="stats-grid">{html_panel}</div></div>
            </div>
        </div>
        
        <div id="site-modal" class="modal-overlay" onclick="handleModalClick(event)">
            <div class="modal-content" onclick="event.stopPropagation()">
                <div class="modal-header">
                    <h2 id="modal-site-name">Site Details</h2>
                    <div style="display:flex; gap:10px;">
                        <button onclick="navigateSite(-1)" style="background:rgba(255,255,255,0.2); border:none; color:white; padding:5px 15px; border-radius:5px; cursor:pointer;">‹</button>
                        <button onclick="navigateSite(1)" style="background:rgba(255,255,255,0.2); border:none; color:white; padding:5px 15px; border-radius:5px; cursor:pointer;">›</button>
                        <button class="modal-close" onclick="closeSiteModal()" style="background:none; border:none; font-size:24px; color:white; cursor:pointer;">&times;</button>
                    </div>
                </div>
                <div class="modal-body" id="modal-body">
                    <div class="time-period-selector">
                        <button class="period-button" onclick="changePeriod(this, '7d')">Last 7 Days</button>
                        <button class="period-button" onclick="changePeriod(this, '30d')">Last 30 Days</button>
                        <button class="period-button active" onclick="changePeriod(this, '90d')">Last 90 Days</button>
                        <button class="period-button" onclick="changePeriod(this, 'all')">All Data</button>
                    </div>
                    <div class="site-info-grid" id="site-info-grid"></div>
                    <div style="display:grid; grid-template-columns:1fr 1fr; gap:1rem; margin-top:1rem;">
                        <div class="chart-wrapper"><h4>Daily Production</h4><canvas id="dailyProductionChart"></canvas></div>
                        <div class="chart-wrapper"><h4>Specific Yield</h4><canvas id="yieldTrendChart"></canvas></div>
                    </div>
                    <div class="stats-summary" id="site-stats-summary"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
    // Data Injection
    const siteData = {json_site_data};
    const allSiteIds = {json_all_ids};
    const degradationData = {json_degradation};
    const provinceStats = {json_province};
    const projectStats = {json_project};
    const panelStats = {json_panel};
    const commissioningData = {json_comm};
    const gridAccessData = {json_grid};
    const powerSourcesData = {json_power};
    
    // Site Lists
    const excellentIds = {json_exc};
    const goodIds = {json_good};
    const fairIds = {json_fair};
    const poorIds = {json_poor};
    
    const offlineIds=[], highDegIds=[], medDegIds=[], lowDegIds=[], betterDegIds=[];
    let currentSiteId=null, currentSiteIndex=0, currentSiteList=[], currentCategory='all', siteCharts=[], currentPeriod='90d';

    function showTab(el, tab) {{
        document.querySelectorAll('.nav-item').forEach(i=>i.classList.remove('active'));
        el.classList.add('active');
        document.querySelectorAll('[id$="-tab"]').forEach(t=>t.classList.add('hidden'));
        document.getElementById(tab+'-tab').classList.remove('hidden');
    }}

    function toggleTheme() {{
        document.body.classList.toggle('dark-mode');
        const b = document.querySelector('.theme-toggle');
        b.innerText = document.body.classList.contains('dark-mode') ? '☀️ Light Mode' : '🌙 Dark Mode';
    }}

    function openSiteModal(id, cat) {{
        currentSiteId = id; currentCategory = cat||'all';
        if(cat==='excellent') currentSiteList=excellentIds;
        else if(cat==='good') currentSiteList=goodIds;
        else if(cat==='fair') currentSiteList=fairIds;
        else if(cat==='poor') currentSiteList=poorIds;
        else if(cat==='offline') currentSiteList=offlineIds;
        else if(cat==='high-degradation') currentSiteList=highDegIds;
        else if(cat==='medium-degradation') currentSiteList=medDegIds;
        else if(cat==='low-degradation') currentSiteList=lowDegIds;
        else if(cat==='better-degradation') currentSiteList=betterDegIds;
        else currentSiteList=allSiteIds;
        
        currentSiteIndex = currentSiteList.indexOf(id);
        const site = siteData[id];
        document.getElementById('modal-site-name').innerText = site.site_name;
        document.getElementById('site-modal').classList.add('active');
        
        const btns = document.querySelectorAll('.period-button');
        btns.forEach(b=>b.classList.remove('active'));
        btns[2].classList.add('active');
        loadSiteData(btns[2], '90d');
    }}

    function closeSiteModal() {{
        document.getElementById('site-modal').classList.remove('active');
        siteCharts.forEach(c=>c.destroy()); siteCharts=[];
    }}

    function handleModalClick(e) {{ if(e.target.id==='site-modal') closeSiteModal(); }}

    function navigateSite(dir) {{
        const newIdx = currentSiteIndex+dir;
        if(newIdx>=0 && newIdx<currentSiteList.length) openSiteModal(currentSiteList[newIdx], currentCategory);
    }}

    function changePeriod(btn, period) {{
        currentPeriod = period;
        document.querySelectorAll('.period-button').forEach(b=>b.classList.remove('active'));
        btn.classList.add('active');
        loadSiteData(btn, period);
    }}

    function loadSiteData(btn, period) {{
        const site = siteData[currentSiteId];
        const now = new Date();
        const days = {{'7d':7, '30d':30, '90d':90}};
        let data = site.daily_data;
        
        if(period!=='all') {{
            const cut = new Date(now-days[period]*24*60*60*1000);
            data = data.filter(d=>new Date(d.date)>=cut);
        }}
        const valid = data.filter(d=>!isNaN(d.solar_supply_kwh));
        
        document.getElementById('site-info-grid').innerHTML = `
            <div class="site-info-item"><div class="site-info-label">Panel</div><div class="site-info-value">${{site.panel_description}}</div></div>
            <div class="site-info-item"><div class="site-info-label">Size</div><div class="site-info-value">${{site.array_size_kwp.toFixed(2)}} kWp</div></div>
            <div class="site-info-item"><div class="site-info-label">Project</div><div class="site-info-value">${{site.project}}</div></div>
            <div class="site-info-item"><div class="site-info-label">Grid</div><div class="site-info-value">${{site.grid_access}}</div></div>
            <div class="site-info-item"><div class="site-info-label">Province</div><div class="site-info-value">${{site.province}}</div></div>
            <div class="site-info-item"><div class="site-info-label">Comm.</div><div class="site-info-value">${{site.commissioned_date}}</div></div>`;
        
        const total = valid.reduce((a,b)=>a+b.solar_supply_kwh,0);
        const avg = valid.length ? valid.reduce((a,b)=>a+b.specific_yield,0)/valid.length : 0;
        
        document.getElementById('site-stats-summary').innerHTML = `
            <div class="summary-card"><div class="summary-label">Total</div><div class="summary-value">${{total.toFixed(0)}}</div></div>
            <div class="summary-card green"><div class="summary-label">Avg Yield</div><div class="summary-value">${{avg.toFixed(2)}}</div></div>`;

        if(siteCharts.length) {{ siteCharts.forEach(c=>c.destroy()); siteCharts=[]; }}
        siteCharts.push(new Chart(document.getElementById('dailyProductionChart').getContext('2d'), {{
            type:'line', data:{{ labels:valid.map(d=>d.date), datasets:[{{ label:'kWh', data:valid.map(d=>d.solar_supply_kwh), borderColor:'#3498db', backgroundColor:'rgba(52,152,219,0.1)', fill:true }}] }},
            options:{{ responsive:true, maintainAspectRatio:false, scales:{{ y:{{ beginAtZero:true }} }} }}
        }}));
        siteCharts.push(new Chart(document.getElementById('yieldTrendChart').getContext('2d'), {{
            type:'line', data:{{ labels:valid.map(d=>d.date), datasets:[{{ label:'Yield', data:valid.map(d=>d.specific_yield), borderColor:'#27ae60', backgroundColor:'rgba(39,174,96,0.1)', fill:true }}] }},
            options:{{ responsive:true, maintainAspectRatio:false, scales:{{ y:{{ beginAtZero:true }} }} }}
        }}));
    }}

    window.onload = function() {{
        new Chart(document.getElementById('gridAccessChart'), {{ type:'pie', data:{{ labels:gridAccessData.map(d=>d.grid_access), datasets:[{{ data:gridAccessData.map(d=>d.site_count), backgroundColor:['#3498db','#27ae60','#f39c12','#e74c3c','#9b59b6'] }}] }} }});
        new Chart(document.getElementById('powerSourcesChart'), {{ type:'pie', data:{{ labels:powerSourcesData.map(d=>d.power_sources), datasets:[{{ data:powerSourcesData.map(d=>d.site_count), backgroundColor:['#e74c3c','#3498db','#27ae60','#f39c12','#9b59b6'] }}] }} }});
        new Chart(document.getElementById('distChart'), {{ type:'doughnut', data:{{ labels:['Excellent','Good','Fair','Poor'], datasets:[{{ data:[{len(excellent_sites)},{len(good_sites)},{len(fair_sites)},{len(poor_sites)}], backgroundColor:['#27ae60','#3498db','#f39c12','#e74c3c'] }}] }} }});
        new Chart(document.getElementById('commissioningChart'), {{ type:'line', data:{{ labels:commissioningData.map(d=>d.First_Production_Date), datasets:[{{ label:'Sites', data:commissioningData.map(d=>d.cumulative_count), borderColor:'#3498db', fill:true }}] }} }});

        if(degradationData.length > 0) {{
            degradationData.forEach(d => {{
                if(!d.has_recent_data) offlineIds.push(d.site_id);
                else if(d.actual_degradation > 50) highDegIds.push(d.site_id);
                else if(d.actual_degradation >= 30) medDegIds.push(d.site_id);
                else if(d.actual_degradation >= 0) lowDegIds.push(d.site_id);
                else betterDegIds.push(d.site_id);
            }});
            const mkHtml = (s,c,cat) => `<div onclick="openSiteModal('${{s.site_id}}','${{cat}}')" style="padding:0.75rem; border-left:3px solid ${{c}}; background:#f8f9fa; margin-bottom:0.5rem; cursor:pointer; border-radius:0.5rem;"><div style="display:flex; justify-content:space-between;"><div style="font-weight:600;">${{s.site_name}}</div><div style="font-weight:bold;">${{s.actual_degradation.toFixed(1)}}%</div></div><div style="font-size:0.8em; color:#666;">${{s.panel_description}} • ${{s.array_size.toFixed(1)}} kWp</div></div>`;
            const fill = (id, list, c, cat) => {{
                const el = document.getElementById(id);
                const sites = degradationData.filter(d => list.includes(d.site_id));
                el.innerHTML = sites.length ? sites.map(s=>mkHtml(s,c,cat)).join('') : '<div style="padding:1rem; color:#666;">None</div>';
            }};
            fill('offline-sites-list', offlineIds, '#e74c3c', 'offline');
            fill('high-degradation-list', highDegIds, '#e74c3c', 'high-degradation');
            fill('medium-degradation-list', medDegIds, '#f39c12', 'medium-degradation');
            fill('low-degradation-list', lowDegIds, '#27ae60', 'low-degradation');
            fill('better-degradation-list', betterDegIds, '#27ae60', 'better-degradation');
        }}
    }};
    
    // --- EXPORT FUNCTIONS ---
    function exportToExcel(data, name, sheet="Sheet1") {{
        if(!data || !data.length) {{ alert("No data"); return; }}
        const ws = XLSX.utils.json_to_sheet(data);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, sheet);
        XLSX.writeFile(wb, name + ".xlsx");
    }}
    function exportOverview() {{ exportToExcel(commissioningData, "Commissioning_Timeline"); }}
    function exportAllSites() {{
        const list = Object.values(siteData).map(s => {{ const {{daily_data, ...rest}} = s; return rest; }});
        exportToExcel(list, "Master_Site_List");
    }}
    function exportDegradation() {{ exportToExcel(degradationData, "Degradation_Report"); }}
    function exportPerformanceStats() {{
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(provinceStats), "Province");
        XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(projectStats), "Project");
        XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(panelStats), "Panel");
        XLSX.writeFile(wb, "Performance_Stats.xlsx");
    }}
    </script>
</body>
</html>"""
    
    # --- SAVE FILE ---
    # Create output path based on input file location (or current directory)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    # If running locally/manually, default to current directory
    out_path = f"installed_sites_dashboard_{timestamp}.html"
    
    # Save HTML
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
        
    print(f"\n✓ Dashboard generated successfully: {out_path}")

if __name__ == "__main__":
    main()
