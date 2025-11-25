import os
import json
import shutil
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
from oauth2client.client import GoogleCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# --- CONFIGURATION (UPDATE THESE!) ---
FOLDER_ID_DASHBOARD = "14hr8Jwhfi0deKHjWE3aUpqKo2OpGk-MC"  
FOLDER_ID_MONITORING = "1L3AmxhEdXc_W6J9zrMtRPv6a0VpsjyXo" 
FOLDER_ID_ARCHIVE = "1_L1QSEdxm_skS7vgLWGaD1WRYjpxOsbI" 

# --- HELPER FUNCTIONS (FROM YOUR FILE) ---
PROVINCE_MAPPING = {
    'SV': 'Sihanoukville', 'KK': 'Koh Kong', 'SI': 'Siem Reap', 'PV': 'Prey Veng',
    'SR': 'Svay Rieng', 'KD': 'Kandal', 'KS': 'Kampong Speu', 'KC': 'Kampong Cham',
    'KH': 'Kampong Chhnang', 'BB': 'Battambang', 'PS': 'Pursat', 'PH': 'Preah Vihear',
    'KT': 'Kampong Thom', 'PL': 'Pailin', 'BM': 'Banteay Meanchey', 'TB': 'Tboung Khmum',
    'OM': 'Oddar Meanchey', 'KP': 'Kampot', 'KE': 'Kep', 'KR': 'Kratie',
    'ST': 'Stung Treng', 'MK': 'Mondulkiri', 'RK': 'Ratanakiri', 'PP': 'Phnom Penh', 'TK': 'Takeo'
}

def get_province_full_name(abbreviation):
    return PROVINCE_MAPPING.get(abbreviation.upper(), abbreviation)

def extract_province_from_site_id(site_id):
    if isinstance(site_id, str) and len(site_id) >= 2:
        return site_id[:2].upper()
    return 'Unknown'
   
def safe_val(val, func=float, default=0):
    try:
        return func(val) if pd.notna(val) else default
    except:
        return default
   
# --- AUTHENTICATION ---
def authenticate_drive():
    print("Authenticating with Google Drive (OAuth2)...")
    client_id = os.environ.get("GDRIVE_CLIENT_ID")
    client_secret = os.environ.get("GDRIVE_CLIENT_SECRET")
    refresh_token = os.environ.get("GDRIVE_REFRESH_TOKEN")
    
    if not client_id or not client_secret or not refresh_token:
        raise Exception("Missing OAuth2 Secrets in GitHub!")

    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials(
        access_token=None,
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        token_expiry=None,
        token_uri="https://oauth2.googleapis.com/token",
        user_agent=None
    )
    return GoogleDrive(gauth)

# --- PART 1: DATA PROCESSOR ---
def run_processor(drive):
    print("\n" + "="*30)
    print("STARTING DATA PROCESSOR")
    print("="*30)
    
    # 1. Download Metadata
    print("Downloading Metadata...")
    meta_files = drive.ListFile({'q': f"'{FOLDER_ID_DASHBOARD}' in parents and title = 'Solar Installation info.xlsx' and trashed=false"}).GetList()
    
    if not meta_files:
        print("❌ Critical Error: Metadata file 'Solar Installation info.xlsx' not found.")
        return None
        
    meta_file = meta_files[0]
    meta_file.GetContentFile("metadata.xlsx")
    
    # 2. Download History
    print("Downloading History...")
    hist_files = drive.ListFile({'q': f"'{FOLDER_ID_DASHBOARD}' in parents and title = 'all_monitoring_data_long.parquet' and trashed=false"}).GetList()
    
    if hist_files:
        hist_files[0].GetContentFile("history.parquet")
        history_df = pd.read_parquet("history.parquet")
        print(f"  ✓ Loaded {len(history_df):,} historical records.")
    else:
        print("  ⚠ No history file found. Starting fresh.")
        history_df = pd.DataFrame()
        
    # 3. Download NEW Monitoring Files
    print("Checking for new files in Monitoring Data folder...")
    new_files = drive.ListFile({'q': f"'{FOLDER_ID_MONITORING}' in parents and trashed=false"}).GetList()
    data_files = [f for f in new_files if f['title'].endswith(('.xlsx', '.csv'))]
    
    if not data_files:
        print("  ✓ No new files found. Nothing to process.")
        return None 

    new_dfs = []
    processed_files = []

    for f in data_files:
        print(f"  Processing: {f['title']}...")
        f.GetContentFile(f['title'])
        
        try:
            df = None
            if f['title'].endswith('.xlsx'):
                df_temp = pd.read_excel(f['title'], header=None, nrows=20)
                header_row = 0
                for i, row in df_temp.iterrows():
                    if str(row.iloc[0]).strip() == 'Site': 
                        header_row = i
                        break
                df = pd.read_excel(f['title'], skiprows=header_row)
                
            elif f['title'].endswith('.csv'):
                df = pd.read_csv(f['title'], sep='\t', encoding='utf-16', on_bad_lines='skip')
            
            if df is not None:
                df.columns = [str(c).replace('\ufeff', '').strip() for c in df.columns]
                
                if 'Site' in df.columns:
                    renames = {'Site ID': 'Site_ID', 'Solar Supply (kWh)': 'Solar_kWh'}
                    df = df.rename(columns=renames)
                    
                    if 'Site_ID' not in df.columns: df['Site_ID'] = df['Site']
                    
                    if 'Date' in df.columns and 'Solar_kWh' in df.columns:
                        df = df[['Site_ID', 'Date', 'Solar_kWh']].dropna()
                        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                        df['Solar_kWh'] = pd.to_numeric(df['Solar_kWh'], errors='coerce')
                        
                        new_dfs.append(df)
                        processed_files.append(f)
                        print(f"    ✓ Extracted {len(df)} rows.")
                    else:
                        print("    ⚠ Missing required columns (Date or Solar_kWh).")
        except Exception as e:
            print(f"    ✗ Error reading file: {e}")

    # 4. Combine & Save
    if new_dfs:
        print("Updating History...")
        new_combined = pd.concat(new_dfs, ignore_index=True)
        full_df = pd.concat([history_df, new_combined], ignore_index=True)
        full_df = full_df.sort_values('Date').drop_duplicates(subset=['Site_ID', 'Date'], keep='last')
        print(f"  ✓ Total records after merge: {len(full_df):,}")
        
        full_df.to_parquet("history.parquet", index=False)
        
        if hist_files:
            f_hist = hist_files[0]
        else:
            f_hist = drive.CreateFile({'title': 'all_monitoring_data_long.parquet', 'parents': [{'id': FOLDER_ID_DASHBOARD}]})
            
        f_hist.SetContentFile("history.parquet")
        f_hist.Upload()
        print("  ✓ History Parquet uploaded to Drive.")
        
        # 5. Generate Pivot Excel
        print("Generating Production Excel...")
        metadata = pd.read_excel("metadata.xlsx")
        metadata['Site_ID'] = metadata['Split'].astype(str).str.strip()
        metadata['Panels'] = pd.to_numeric(metadata['Panels'], errors='coerce')
        metadata['Panel Size'] = pd.to_numeric(metadata['Panel Size'], errors='coerce')
        metadata['Array_Size_kWp'] = metadata['Panels'] * metadata['Panel Size'] / 1000
        
        metadata['Panel_Description'] = metadata.apply(
            lambda r: f"{int(r['Panel Size']) if pd.notna(r['Panel Size']) else '?'} {str(r['Panel Vendor'])} {str(r['Panel Model'])}", axis=1
        )
        
        pivot = full_df.pivot(index='Site_ID', columns='Date', values='Solar_kWh').reset_index()
        final = metadata.merge(pivot, on='Site_ID', how='left')
        
        # Fix: Calculate First Production Date
        date_cols = [c for c in final.columns if isinstance(c, pd.Timestamp)]
        def get_first_date(row):
            for col in date_cols:
                if pd.notna(row[col]) and row[col] > 0: return col
            return None
        final['First_Production_Date'] = final.apply(get_first_date, axis=1)
        final = final.rename(columns={c: c.strftime('%Y-%m-%d') for c in date_cols})
        
        ts = datetime.now().strftime('%Y%m%d%H%M%S')
        out_name = f"installed_sites_production{ts}.xlsx"
        final.to_excel(out_name, index=False)
        
        f_out = drive.CreateFile({'title': out_name, 'parents': [{'id': FOLDER_ID_DASHBOARD}]})
        f_out.SetContentFile(out_name)
        f_out.Upload()
        print(f"  ✓ Production Report uploaded: {out_name}")
        
        # 6. Archive Files
        print("Archiving processed files...")
        for pf in processed_files:
            pf['parents'] = [{'id': FOLDER_ID_ARCHIVE}]
            pf.Upload()
        print("  ✓ Files moved to Processed_Archive.")
        
        return out_name
    return None

# --- PART 2: DASHBOARD GENERATOR (PRESERVING YOUR LOGIC) ---
def run_dashboard(drive, excel_filename):
    if not excel_filename: return
    print("\n" + "="*30 + "\nSTARTING DASHBOARD GENERATOR\n" + "="*30)
    
    # 1. Load Data
    df = pd.read_excel(excel_filename) 
    
    # 2. Download DB
    print("Downloading Database...")
    db_files = drive.ListFile({'q': f"'{FOLDER_ID_DASHBOARD}' in parents and title = 'solar_performance.db'"}).GetList()
    db_path = "solar_performance.db"
    if db_files: 
        db_files[0].GetContentFile(db_path)
        print("  ✓ Database downloaded.")
    else: 
        print("  ⚠ Database not found in Drive. Enrichment will be skipped.")
        db_path = None

    # --- DATA PROCESSING LOGIC (YOUR EXACT FEATURES) ---
    site_name_map = {}
    site_commissioned_map = {}
    if db_path and os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            site_df_db = pd.read_sql_query("SELECT site_id, site_name, commissioned_date FROM sites", conn)
            site_name_map = dict(zip(site_df_db['site_id'], site_df_db['site_name']))
            site_commissioned_map = dict(zip(site_df_db['site_id'], site_df_db['commissioned_date']))
            conn.close()
        except Exception as e:
            print(f"  ⚠ DB Error: {e}")

    df['Province'] = df['Site_ID'].apply(extract_province_from_site_id)
    df['Province_Full'] = df['Province'].apply(get_province_full_name)
    
    date_cols = [c for c in df.columns if isinstance(c, str) and len(c) == 10 and c[4] == '-' and c[7] == '-']
    date_cols_sorted = sorted(date_cols, reverse=True)
    date_cols_dt = {col: pd.to_datetime(col) for col in date_cols}
    
    # Degradation Logic
    print(f"  Calculating degradation metrics...")
    degradation_data = []
    for idx, row in df.iterrows():
        if pd.isna(row['Array_Size_kWp']) or row['Array_Size_kWp'] == 0: continue
        
        # Fix: Handle missing/string dates safely
        first_date_val = row.get('First_Production_Date')
        if pd.isna(first_date_val): continue
        try: first_date = pd.to_datetime(first_date_val)
        except: continue
        
        latest_date = pd.to_datetime(date_cols_sorted[0]) if date_cols_sorted else None
        if not latest_date: continue
        
        comm_end = first_date + timedelta(days=30)
        last_start = latest_date - timedelta(days=30)
        
        c_cols = [c for c in date_cols if first_date <= date_cols_dt[c] <= comm_end]
        l_cols = [c for c in date_cols if last_start <= date_cols_dt[c] <= latest_date]
        
        if c_cols and l_cols:
            c_vals = [row[c] for c in c_cols if pd.notna(row[c]) and row[c]>0]
            l_vals = [row[c] for c in l_cols if pd.notna(row[c]) and row[c]>0]
            if len(c_vals) >= 5 and len(l_vals) >= 5:
                init_95 = np.percentile(c_vals, 95) / row['Array_Size_kWp']
                curr_95 = np.percentile(l_vals, 95) / row['Array_Size_kWp']
                years = (latest_date - first_date).days / 365.25
                exp = years * 1.5 if years <= 1 else 1.5 + (years-1)*0.4
                act = (init_95 - curr_95)/init_95 * 100 if init_95 > 0 else 0
                
                degradation_data.append({
                    'site_id': row['Site_ID'],
                    'site_name': site_name_map.get(row['Site_ID'], str(row.get('Site', row['Site_ID']))),
                    'actual_degradation': act,
                    'expected_degradation': exp,
                    'performance_vs_expected': exp - act,
                    'years_elapsed': years,
                    'panel_description': str(row.get('Panel_Description', 'N/A')),
                    'array_size': row['Array_Size_kWp'],
                    'province': row['Province_Full'],
                    'has_recent_data': any(pd.notna(row[d]) and row[d]>0 for d in date_cols_sorted[:3])
                })
                
    degradation_df = pd.DataFrame(degradation_data)
    print(f"  ✓ Degradation analysis complete for {len(degradation_df)} sites")
    
    # Stats Calculation (Preserved Logic)
    total_sites = len(df)
    sites_with_data = len(df[df['Days_With_Data'] > 0]) if 'Days_With_Data' in df.columns else 0
    total_capacity = df['Array_Size_kWp'].sum()
    
    df_calc = df.fillna(0)
    if total_capacity > 0:
        avg_yield_30d = (df_calc['Avg_Yield_30d_kWh_kWp'] * df_calc['Array_Size_kWp']).sum() / total_capacity
        avg_yield_7d = (df_calc.get('Avg_Yield_7d_kWh_kWp', 0) * df_calc['Array_Size_kWp']).sum() / total_capacity
        avg_yield_90d = (df_calc.get('Avg_Yield_90d_kWh_kWp', 0) * df_calc['Array_Size_kWp']).sum() / total_capacity
    else:
        avg_yield_30d = avg_yield_7d = avg_yield_90d = 0
    
    last_3_days = date_cols_sorted[:3] if len(date_cols_sorted) >= 3 else date_cols_sorted
    critical_alerts = []
    for idx, row in df.iterrows():
        if row.get('Days_With_Data', 0) > 0:
             if all(pd.isna(row[d]) or row[d] == 0 for d in last_3_days):
                critical_alerts.append(row['Site_ID'])
    
    # Categories
    excellent_sites = df[df['Avg_Yield_30d_kWh_kWp'] > 4.5].fillna(0).to_dict('records')
    good_sites = df[(df['Avg_Yield_30d_kWh_kWp'] > 3.5) & (df['Avg_Yield_30d_kWh_kWp'] <= 4.5)].fillna(0).to_dict('records')
    fair_sites = df[(df['Avg_Yield_30d_kWh_kWp'] > 2.5) & (df['Avg_Yield_30d_kWh_kWp'] <= 3.5)].fillna(0).to_dict('records')
    poor_sites = df[df['Avg_Yield_30d_kWh_kWp'] <= 2.5].fillna(0).to_dict('records')
    
    # Grouping Logic
    def get_weighted_stats(group_col, label):
        valid = df[df['Array_Size_kWp'] > 0].copy()
        if valid.empty: return pd.DataFrame()
        stats = valid.groupby(group_col).agg(
            site_count=('Site_ID', 'count'),
            total_capacity=('Array_Size_kWp', 'sum'),
            avg_yield=('Avg_Yield_30d_kWh_kWp', lambda x: (x * valid.loc[x.index, 'Array_Size_kWp']).sum() / valid.loc[x.index, 'Array_Size_kWp'].sum())
        ).reset_index().rename(columns={group_col: label}).sort_values('avg_yield', ascending=False)
        return stats

    province_stats = get_weighted_stats('Province_Full', 'province')
    project_stats = get_weighted_stats('Project', 'project')
    panel_stats = get_weighted_stats('Panel_Description', 'panel_type')
    grid_access_stats = df.groupby('Grid Access').agg(site_count=('Site_ID', 'count')).reset_index()
    power_sources_stats = df.groupby('Power Sources').agg(site_count=('Site_ID', 'count')).reset_index()

    # Timeline
    commissioning_timeline = df[df['First_Production_Date'].notna()].copy()
    commissioning_timeline['First_Production_Date'] = pd.to_datetime(commissioning_timeline['First_Production_Date'])
    date_counts = commissioning_timeline.sort_values('First_Production_Date').groupby('First_Production_Date').size().reset_index(name='count')
    date_counts['cumulative_count'] = date_counts['count'].cumsum()
    date_counts['First_Production_Date'] = date_counts['First_Production_Date'].dt.strftime('%Y-%m-%d')
    commissioning_data = date_counts

    print(f"  Total Sites: {total_sites}")
    
    # Prepare Site Data
    site_data = {}
    for idx, row in df.iterrows():
        site_id = row['Site_ID']
        daily_data = []
        for date_col in date_cols:
            if pd.notna(row[date_col]):
                daily_data.append({
                    'date': date_col, 
                    'solar_supply_kwh': float(row[date_col]),
                    'specific_yield': float(row[date_col])/float(row['Array_Size_kWp']) if pd.notna(row['Array_Size_kWp']) and row['Array_Size_kWp'] > 0 else 0
                })
        
        def safe_int(value):
            try: return int(pd.to_numeric(value, errors='coerce')) if pd.notna(value) else 0
            except: return 0
        def safe_float(value):
            try: return float(pd.to_numeric(value, errors='coerce')) if pd.notna(value) else 0
            except: return 0
        
        site_data[site_id] = {
            'site_id': site_id,
            'site_name': site_name_map.get(site_id, str(row.get('Site', site_id))),
            'project': str(row.get('Project', '')),
            'panel_description': str(row.get('Panel_Description', '')),
            'array_size_kwp': safe_float(row['Array_Size_kWp']),
            'province': row['Province_Full'],
            'commissioned_date': site_commissioned_map.get(site_id, str(row.get('First_Production_Date', ''))),
            'daily_data': daily_data,
            'grid_access': str(row.get('Grid Access', '')),
            'power_sources': str(row.get('Power Sources', '')),
            'po': str(row.get('PO', '')),
            'panels': safe_int(row.get('Panels')),
            'panel_size': safe_int(row.get('Panel Size')),
            'panel_vendor': str(row.get('Panel Vendor', '')),
            'panel_model': str(row.get('Panel Model', '')),
            'avg_load': safe_float(row.get('Avg Load', 0))
        }

    print(f"\n[4/4] Generating HTML dashboard...")
    
    # Helper for HTML Lists
    def generate_site_list_item(site, color='blue', category='all'):
        site_name = site_name_map.get(site['Site_ID'], site['Site'])
        color_map = {'green': '#27ae60', 'blue': '#3498db', 'yellow': '#f39c12', 'red': '#e74c3c'}
        return f'''<div class="site-list-item" onclick="openSiteModal('{site['Site_ID']}', '{category}')" style="cursor: pointer; padding: 0.75rem; border-left: 3px solid {color_map[color]}; margin-bottom: 0.5rem; background: #f8f9fa; border-radius: 0.5rem; transition: transform 0.2s;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div style="font-weight: 600;">{site_name}</div>
                <div style="font-weight: bold; color: {color_map[color]};">{site['Avg_Yield_30d_kWh_kWp']:.2f} kWh/kWp/day</div>
            </div>
            <div style="font-size: 0.875rem; color: #6c757d; margin-top: 0.25rem;">{site['Panel_Description']} • {site['Array_Size_kWp']:.1f} kWp</div>
        </div>'''

    excellent_html = ''.join([generate_site_list_item(s, 'green', 'excellent') for s in excellent_sites])
    good_html = ''.join([generate_site_list_item(s, 'blue', 'good') for s in good_sites])
    fair_html = ''.join([generate_site_list_item(s, 'yellow', 'fair') for s in fair_sites])
    poor_html = ''.join([generate_site_list_item(s, 'red', 'poor') for s in poor_sites])

    # Helper for Stats Cards (Province/Project/Panel)
    def generate_card_html(stats_df, label_col):
        html = ""
        for _, p in stats_df.iterrows():
            val = p['avg_yield']
            color = 'green' if val > 4.0 else ('yellow' if val > 3.0 else 'red')
            color_hex = {'green': '#27ae60', 'yellow': '#f39c12', 'red': '#e74c3c'}[color]
            html += f'''<div class="stat-card" style="border-left: 4px solid {color_hex}; background: white; padding: 1rem; border-radius: 0.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <div style="font-size: 0.8rem; font-weight: bold; color: #666;">{p[label_col]}</div>
                <div style="font-size: 1.5rem; font-weight: bold;">{val:.2f}</div>
                <div style="font-size: 0.8rem; color: #666;">{int(p['site_count'])} sites • {p['total_capacity']:.1f} kWp</div>
            </div>'''
        return html

    province_html = generate_card_html(province_stats, 'province')
    project_html = generate_card_html(project_stats, 'project')
    panel_html = generate_card_html(panel_stats, 'panel_type')
    
    all_site_ids = [str(site_id) for site_id in df['Site_ID'].tolist()]

    # --- HTML CONTENT (YOUR EXACT TEMPLATE) ---
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Solar Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
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
            <div class="stats-grid">
                <div class="stat-card blue"><div class="stat-label">Total Sites</div><div class="stat-value">{total_sites}</div><div class="stat-subtitle">{sites_with_data} with production data</div></div>
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
            <div class="chart-container"><h3>🌟 Excellent Performance (>4.5 kWh/kWp/day) - {len(excellent_sites)} sites</h3><div class="scroll-list">{excellent_html}</div></div>
            <div class="chart-container"><h3>✅ Good Performance (3.5-4.5 kWh/kWp/day) - {len(good_sites)} sites</h3><div class="scroll-list">{good_html}</div></div>
            <div class="chart-container"><h3>⚠️ Fair Performance (2.5-3.5 kWh/kWp/day) - {len(fair_sites)} sites</h3><div class="scroll-list">{fair_html}</div></div>
            <div class="chart-container"><h3>🚨 Poor Performance (<2.5 kWh/kWp/day) - {len(poor_sites)} sites</h3><div class="scroll-list">{poor_html}</div></div>
        </div>

        <div id="degradation-tab" class="hidden">
            <div class="chart-container"><h3>🚨 Offline / No Data</h3><div id="offline-sites-list" class="scroll-list"></div></div>
            <div class="chart-container"><h3>🔴 High Degradation (>50%)</h3><div id="high-degradation-list" class="scroll-list"></div></div>
            <div class="chart-container"><h3>⚠️ Medium Degradation (30-50%)</h3><div id="medium-degradation-list" class="scroll-list"></div></div>
            <div class="chart-container"><h3>✅ Low Degradation (0-30%)</h3><div id="low-degradation-list" class="scroll-list"></div></div>
            <div class="chart-container"><h3>🌟 Better Than Expected</h3><div id="better-degradation-list" class="scroll-list"></div></div>
        </div>

        <div id="performance-tab" class="hidden">
            <div class="performance-section province"><h3>🏢 Province Performance</h3><div class="stats-grid">{province_html}</div></div>
            <div class="performance-section project"><h3>📋 Project Performance</h3><div class="stats-grid">{project_html}</div></div>
            <div class="performance-section panel"><h3>⚡ Panel Type Performance</h3><div class="stats-grid">{panel_html}</div></div>
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
    const siteData = {json.dumps(site_data)};
    const allSiteIds = {json.dumps(all_site_ids)};
    const degradationData = {json.dumps(degradation_df.to_dict('records') if len(degradation_df) > 0 else [])};
    const gridAccessData = {json.dumps(grid_access_stats.to_dict('records'))};
    const powerSourcesData = {json.dumps(power_sources_stats.to_dict('records'))};
    const commissioningData = {json.dumps(commissioning_timeline_data[['First_Production_Date', 'cumulative_count', 'count']].to_dict('records') if len(commissioning_timeline_data) > 0 else [])};
    
    const excellentIds = {json.dumps([str(s['Site_ID']) for s in excellent_sites])};
    const goodIds = {json.dumps([str(s['Site_ID']) for s in good_sites])};
    const fairIds = {json.dumps([str(s['Site_ID']) for s in fair_sites])};
    const poorIds = {json.dumps([str(s['Site_ID']) for s in poor_sites])};
    
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
        const s = siteData[id];
        document.getElementById('modal-site-name').innerText = s.site_name;
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
                else if(d.actual_degradation > 50) highDegradationIds.push(d.site_id);
                else if(d.actual_degradation >= 30) mediumDegradationIds.push(d.site_id);
                else if(d.actual_degradation >= 0) lowDegradationIds.push(d.site_id);
                else betterDegradationIds.push(d.site_id);
            }});
            const mkHtml = (s,c,cat) => `<div onclick="openSiteModal('${{s.site_id}}','${{cat}}')" style="padding:0.75rem; border-left:3px solid ${{c}}; background:#f8f9fa; margin-bottom:0.5rem; cursor:pointer; border-radius:0.5rem;"><div style="display:flex; justify-content:space-between;"><div style="font-weight:600;">${{s.site_name}}</div><div style="font-weight:bold;">${{s.actual_degradation.toFixed(1)}}%</div></div><div style="font-size:0.8em; color:#666;">${{s.panel_description}} • ${{s.array_size.toFixed(1)}} kWp</div></div>`;
            const fill = (id, list, c, cat) => {{
                const el = document.getElementById(id);
                const sites = degradationData.filter(d => list.includes(d.site_id));
                el.innerHTML = sites.length ? sites.map(s=>mkHtml(s,c,cat)).join('') : '<div style="padding:1rem; color:#666;">None</div>';
            }};
            fill('offline-sites-list', offlineIds, '#e74c3c', 'offline');
            fill('high-degradation-list', highDegradationIds, '#e74c3c', 'high-degradation');
            fill('medium-degradation-list', mediumDegradationIds, '#f39c12', 'medium-degradation');
            fill('low-degradation-list', lowDegradationIds, '#27ae60', 'low-degradation');
            fill('better-degradation-list', betterDegradationIds, '#27ae60', 'better-degradation');
        }}
    }};
    </script>
</body>
</html>"""
    
    # 3. Upload Result to Google Drive
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    html_name = f"installed_sites_dashboard_{ts}.html"
    
    with open(html_name, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html_content)
        
    print(f"Uploading {html_name}...")
    f_out = drive.CreateFile({'title': html_name, 'parents': [{'id': FOLDER_ID_DASHBOARD}]})
    f_out.SetContentFile(html_name)
    f_out.Upload()
    print(f"  ✓ Dashboard uploaded successfully!")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # 1. Authenticate
    drive_service = authenticate_drive()
    
    # 2. Run Processor (Part 1)
    new_excel_file = run_processor(drive_service)
    
    # 3. Run Dashboard (Part 2)
    if new_excel_file:
        run_dashboard(drive_service, new_excel_file)
    else:
        print("No new data processed, skipping dashboard generation.")
