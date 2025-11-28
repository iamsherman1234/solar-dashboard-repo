import json
import os
import shutil
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
import numpy as np

# --- CONFIGURATION ---
OUTPUT_FOLDER = "mobile_build"

PROVINCE_MAPPING = {
    'SV': 'Sihanoukville', 'KK': 'Koh Kong', 'SI': 'Siem Reap', 'PV': 'Prey Veng',
    'SR': 'Svay Rieng', 'KD': 'Kandal', 'KS': 'Kampong Speu', 'KC': 'Kampong Cham',
    'KH': 'Kampong Chhnang', 'BB': 'Battambang', 'PS': 'Pursat', 'PH': 'Preah Vihear',
    'KT': 'Kampong Thom', 'PL': 'Pailin', 'BM': 'Banteay Meanchey', 'TB': 'Tboung Khmum',
    'OM': 'Oddar Meanchey', 'KP': 'Kampot', 'KE': 'Kep', 'KR': 'Kratie',
    'ST': 'Stung Treng', 'MK': 'Mondulkiri', 'RK': 'Ratanakiri', 'PP': 'Phnom Penh', 'TK': 'Takeo'
}

def get_province_full_name(abbrev):
    return PROVINCE_MAPPING.get(str(abbrev).upper(), str(abbrev))

def generate_mobile_site():
    print("="*70)
    print("FULL-FEATURED MOBILE GENERATOR (ALL FEATURES)")
    print("="*70)
    
    scripts_folder = Path(__file__).parent.resolve()
    output_dir = scripts_folder / OUTPUT_FOLDER
    data_dir = output_dir / "site_data"
    
    # 1. Setup Folders
    if output_dir.exists(): 
        shutil.rmtree(output_dir)
    output_dir.mkdir()
    data_dir.mkdir()

    # 2. Load Data
    excel_files = list(scripts_folder.glob("installed_sites_production_*.xlsx"))
    if not excel_files: 
        return print("✗ No production file found.")
    excel_file = max(excel_files, key=lambda p: p.stat().st_mtime)
    print(f"  Reading: {excel_file.name}")
    
    try:
        df = pd.read_excel(excel_file, sheet_name='Installed Sites Production')
    except:
        return print("✗ Error reading Excel")

    # 3. Load DB Extra Info
    site_db_info = {}
    try:
        db_path = scripts_folder / "solar_performance.db"
        if db_path.exists():
            conn = sqlite3.connect(db_path)
            temp = pd.read_sql("SELECT site_id, site_name, commissioned_date FROM sites", conn)
            conn.close()
            site_db_info = temp.set_index('site_id').to_dict('index')
    except: 
        pass

    # 4. Pre-process Columns
    def safe_get(row, key, default=0, type_func=float):
        val = row.get(key, default)
        try: 
            return type_func(val) if pd.notna(val) else default
        except: 
            return default

    df['Province_Full'] = df['Site_ID'].astype(str).str[:2].apply(get_province_full_name)
    date_cols = sorted([c for c in df.columns if isinstance(c, str) and len(c)==10 and c[4]=='-'], reverse=True)
    col_to_date = {c: pd.to_datetime(c) for c in date_cols}
    latest_date = col_to_date[date_cols[0]] if date_cols else datetime.now()

    # 5. Calculate Degradation Analysis (COMPLETE IMPLEMENTATION)
    print(f"\n  Calculating degradation metrics for {len(df)} sites...")
    degradation_data = []
    
    for idx, row in df.iterrows():
        site_id = str(row['Site_ID'])
        array_size = safe_get(row, 'Array_Size_kWp')
        
        if array_size <= 0:
            continue
            
        # Get first production date
        first_date = row['First_Production_Date']
        if pd.isna(first_date):
            continue
            
        first_date = pd.to_datetime(first_date)
        
        # Define commissioning month and last month
        commissioning_month_end = first_date + pd.DateOffset(months=1)
        last_month_start = latest_date - pd.DateOffset(months=1)
        
        # Get date columns for each period
        commissioning_cols = [col for col in date_cols
                             if first_date <= col_to_date[col] < commissioning_month_end]
        last_month_cols = [col for col in date_cols
                          if last_month_start <= col_to_date[col] <= latest_date]
        
        # Calculate 95th percentile for each period
        if commissioning_cols and last_month_cols:
            commissioning_values = [row[col] for col in commissioning_cols 
                                   if pd.notna(row[col]) and row[col] > 0]
            last_month_values = [row[col] for col in last_month_cols 
                                if pd.notna(row[col]) and row[col] > 0]
            
            if commissioning_values and last_month_values:
                initial_95th = np.percentile(commissioning_values, 95) / array_size
                latest_95th = np.percentile(last_month_values, 95) / array_size
                
                # Calculate years elapsed
                years_elapsed = (latest_date - first_date).days / 365.25
                
                # Calculate expected degradation (same as desktop)
                if years_elapsed <= 1:
                    expected_degradation = years_elapsed * 1.5
                else:
                    expected_degradation = 1.5 + (years_elapsed - 1) * 0.4
                
                # Calculate actual degradation
                actual_degradation = ((initial_95th - latest_95th) / initial_95th * 100) if initial_95th > 0 else 0
                
                # Calculate performance vs expected
                performance_vs_expected = expected_degradation - actual_degradation
                
                # Check if site has data in last 3 days
                last_3_days = date_cols[:3] if len(date_cols) >= 3 else date_cols
                has_recent_data = any(pd.notna(row[date]) and row[date] > 0 for date in last_3_days)
                
                degradation_data.append({
                    'site_id': site_id,
                    'site_name': site_db_info.get(site_id, {}).get('site_name', str(row.get('Site', site_id))),
                    'array_size': array_size,
                    'panel_description': str(row.get('Panel_Description', 'N/A')),
                    'province': row['Province_Full'],
                    'initial_yield_95th': round(initial_95th, 2),
                    'latest_yield_95th': round(latest_95th, 2),
                    'years_elapsed': round(years_elapsed, 2),
                    'expected_degradation': round(expected_degradation, 1),
                    'actual_degradation': round(actual_degradation, 1),
                    'performance_vs_expected': round(performance_vs_expected, 1),
                    'has_recent_data': has_recent_data,
                    'commissioned_date': first_date.strftime('%Y-%m-%d')
                })

    # Save degradation data to separate JSON file
    with open(data_dir / "degradation_data.json", 'w') as f:
        json.dump(degradation_data, f)
    
    print(f"  ✓ Degradation analysis complete for {len(degradation_data)} sites")

    # 6. Global Stats Containers
    site_metadata = {}
    
    # Aggregators
    fleet_stats = {
        'total_sites': len(df),
        'online_sites': 0,
        'capacity': df['Array_Size_kWp'].sum(),
        'avg_yield_7d': df['Avg_Yield_7d_kWh_kWp'].mean(),
        'avg_yield_30d': df['Avg_Yield_30d_kWh_kWp'].mean(),
        'avg_yield_90d': df['Avg_Yield_90d_kWh_kWp'].mean(),
        'critical_alerts': 0,
        'perf_dist': {'Excellent':0, 'Good':0, 'Fair':0, 'Poor':0}
    }
    
    chart_data = {
        'grid_access': df['Grid Access'].fillna('Unknown').value_counts().to_dict(),
        'power_sources': df['Power Sources'].fillna('Unknown').value_counts().to_dict(),
        'commissioning': {}
    }

    # Timeline Logic - Cumulative commissioning
    comm_dates = pd.to_datetime(df['First_Production_Date'], errors='coerce').dropna().sort_values()
    if len(comm_dates) > 0:
        date_counts = comm_dates.value_counts().sort_index()
        cumulative_counts = date_counts.cumsum()
        chart_data['commissioning'] = {k.strftime('%Y-%m-%d'): int(v) for k, v in cumulative_counts.items()}

    print(f"  Processing {len(df)} sites...")

    # Site category lists for navigation
    site_categories = {
        'excellent': [],
        'good': [],
        'fair': [],
        'poor': []
    }

    for _, row in df.iterrows():
        sid = str(row['Site_ID'])
        size = safe_get(row, 'Array_Size_kWp')
        if size <= 0: 
            continue

        # Basic Info (LIGHTWEIGHT - no daily data here)
        meta = {
            'id': sid,
            'name': site_db_info.get(sid, {}).get('site_name', str(row.get('Site', sid))),
            'prov': row['Province_Full'],
            'kwp': round(size, 2),
            'yld30': round(safe_get(row, 'Avg_Yield_30d_kWh_kWp'), 2),
            'yld7': round(safe_get(row, 'Avg_Yield_7d_kWh_kWp'), 2),
            'yld90': round(safe_get(row, 'Avg_Yield_90d_kWh_kWp'), 2),
            'panel': str(row.get('Panel_Description', 'N/A')),
            'proj': str(row.get('Project', 'N/A')),
            'grid': str(row.get('Grid Access', 'N/A')),
            'src': str(row.get('Power Sources', 'N/A')),
            'load': round(safe_get(row, 'Avg Load'), 1),
            'comm': str(row.get('First_Production_Date', 'N/A')),
            'p7': round(safe_get(row, 'Prod_7d_kWh'), 1),
            'p30': round(safe_get(row, 'Prod_30d_kWh'), 1),
            'p90': round(safe_get(row, 'Prod_90d_kWh'), 1),
            'panels': int(safe_get(row, 'Panels', 0, int)),
            'panel_size': int(safe_get(row, 'Panel Size', 0, int)),
            'panel_model': str(row.get('Panel Model', 'N/A')),
            'panel_vendor': str(row.get('Panel Vendor', 'N/A')),
            'po': str(row.get('PO', 'N/A'))
        }
        
        # Performance Category
        if meta['yld30'] > 4.5: 
            cat = 'Excellent'
            site_categories['excellent'].append(sid)
        elif meta['yld30'] >= 3.5: 
            cat = 'Good'
            site_categories['good'].append(sid)
        elif meta['yld30'] >= 2.5: 
            cat = 'Fair'
            site_categories['fair'].append(sid)
        else: 
            cat = 'Poor'
            site_categories['poor'].append(sid)
            
        meta['cat'] = cat
        fleet_stats['perf_dist'][cat] += 1

        # Check Offline (Last 3 days 0)
        recent_cols = date_cols[:3]
        is_online = any(safe_get(row, d) > 0 for d in recent_cols)
        meta['online'] = is_online
        
        if is_online: 
            fleet_stats['online_sites'] += 1
        else: 
            fleet_stats['critical_alerts'] += 1

        site_metadata[sid] = meta

        # HEAVY DATA -> Separate JSON FILE (keep only last 90 days for mobile)
        daily_hist = []
        for d in date_cols[:90]:  # Limit to 90 days for mobile optimization
            if pd.notna(row[d]):
                val = float(row[d])
                daily_hist.append({
                    'd': d, 
                    'v': round(val, 1),  # Round to 1 decimal
                    'y': round(val/size, 2) if size else 0
                })
        
        # Save individual site data
        with open(data_dir / f"{sid}.json", 'w') as f:
            json.dump({
                'meta': meta, 
                'hist': daily_hist
            }, f, separators=(',', ':'))  # Compact JSON

    # 7. Aggregates
    provinces = df.groupby('Province_Full')['Avg_Yield_30d_kWh_kWp'].mean().round(2).to_dict()
    projects = df.groupby('Project')['Avg_Yield_30d_kWh_kWp'].mean().round(2).to_dict()
    panels = df.groupby('Panel_Description')['Avg_Yield_30d_kWh_kWp'].mean().round(2).to_dict()
    
    print(f"  ✓ Processed {len(site_metadata)} sites")
    print(f"  ✓ Generated {len(list(data_dir.glob('*.json')))} data files")
    
    # 8. Generate HTML (NEXT PART - this is getting long, continues in Part 2)
    # The HTML will be in Part 2 with all features implemented

if __name__ == "__main__":
    generate_mobile_site()
