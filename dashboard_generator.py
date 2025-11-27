import json
import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
import numpy as np

# Province mapping from abbreviations to full names
PROVINCE_MAPPING = {
    'SV': 'Sihanoukville', 'KK': 'Koh Kong', 'SI': 'Siem Reap', 'PV': 'Prey Veng',
    'SR': 'Svay Rieng', 'KD': 'Kandal', 'KS': 'Kampong Speu', 'KC': 'Kampong Cham',
    'KH': 'Kampong Chhnang', 'BB': 'Battambang', 'PS': 'Pursat', 'PH': 'Preah Vihear',
    'KT': 'Kampong Thom', 'PL': 'Pailin', 'BM': 'Banteay Meanchey', 'TB': 'Tboung Khmum',
    'OM': 'Oddar Meanchey', 'KP': 'Kampot', 'KE': 'Kep', 'KR': 'Kratie',
    'ST': 'Stung Treng', 'MK': 'Mondulkiri', 'RK': 'Ratanakiri', 'PP': 'Phnom Penh', 'TK': 'Takeo'
}

def get_province_full_name(abbreviation):
    """Convert province abbreviation to full name"""
    return PROVINCE_MAPPING.get(abbreviation.upper(), abbreviation)

def extract_province_from_site_id(site_id):
    """Extract province abbreviation from site ID (first 2 letters)"""
    if isinstance(site_id, str) and len(site_id) >= 2:
        return site_id[:2].upper()
    return 'Unknown'

def generate_installed_sites_dashboard():
    """Generate an HTML dashboard with actual data from installed_sites_production.xlsx"""
    
    print("="*70)
    print("INSTALLED SITES DASHBOARD GENERATOR")
    print("="*70)
    
    # Paths
    scripts_folder = Path(__file__).parent

    # Find the most recent installed_sites_production file
    excel_files = list(scripts_folder.glob("installed_sites_production_*.xlsx"))
    if excel_files:
        excel_file = max(excel_files, key=lambda p: p.stat().st_mtime)
        print(f"  Found: {excel_file.name}")
    else:
        print(f"\n✗ ERROR: No installed_sites_production_*.xlsx files found")
        print("\nPlease run 'sites_table_nogui.py' first.")
        return

    timestamp = datetime.now().strftime('%d%m%Y')
    output_file = scripts_folder / f"installed_sites_dashboard_{timestamp}.html"
    
        
    print(f"\n[1/4] Loading data from {excel_file.name}...")
    
    try:
        df = pd.read_excel(excel_file, sheet_name='Installed Sites Production')
        print(f"✓ Data loaded successfully: {len(df)} sites")
    except Exception as e:
        print(f"✗ Error loading Excel: {e}")
        return
    
    print(f"\n[2/4] Connecting to database for additional site information...")
    
    # Connect to database to get additional site information
    try:
        project_folder = r"C:\Users\chum.layan\OneDrive - Smart Axiata Co., Ltd\Smart\Code\Solar Dashboard"
        db_path = os.path.join(project_folder, "solar_performance.db")
        conn = sqlite3.connect(db_path)
        
        # Get site mapping with additional info
        site_query = "SELECT site_id, site_name, commissioned_date FROM sites"
        site_df_db = pd.read_sql_query(site_query, conn)
        site_name_map = dict(zip(site_df_db['site_id'], site_df_db['site_name']))
        site_commissioned_map = dict(zip(site_df_db['site_id'], site_df_db['commissioned_date']))
        conn.close()
        print("✓ Additional site information loaded from database")
    except Exception as e:
        print(f"⚠ Warning: Could not load additional site info from database: {e}")
        site_name_map = {}
        site_commissioned_map = {}
    
    print(f"\n[3/4] Processing data...")
    
    # Extract province from Site_ID
    df['Province'] = df['Site_ID'].apply(extract_province_from_site_id)
    df['Province_Full'] = df['Province'].apply(get_province_full_name)
    
    # Get date columns
    date_cols = [col for col in df.columns if isinstance(col, str) and len(col) == 10 and col[4] == '-' and col[7] == '-']
    date_cols_sorted = sorted(date_cols, reverse=True)
    
    # Calculate degradation analysis
    print(f"\n  Calculating degradation metrics...")
    degradation_data = []
    
    print(f"  Processing {len(df)} sites for degradation analysis...")
    for idx, row in df.iterrows():
        site_id = row['Site_ID']
    
        # Show progress every 100 sites
        if (idx + 1) % 100 == 0:
            print(f"    Progress: {idx + 1}/{len(df)} sites processed...")
        array_size = row['Array_Size_kWp']
        
        if pd.isna(array_size) or array_size == 0:
            continue
            
        # Get first production date (commissioning month)
        first_date = row['First_Production_Date']
        if pd.isna(first_date):
            continue
            
        first_date = pd.to_datetime(first_date)
        
        # Get data from commissioning month
        commissioning_month_start = first_date
        commissioning_month_end = (first_date + pd.DateOffset(months=1))
        
        # Get data from last month
        latest_date = pd.to_datetime(date_cols[-1]) if date_cols else None
        if latest_date is None:
            continue
            
        last_month_start = latest_date - pd.DateOffset(months=1)
        last_month_end = latest_date
        
        # Filter date columns for each period
        commissioning_cols = [col for col in date_cols
                             if commissioning_month_start <= pd.to_datetime(col) < commissioning_month_end]
        last_month_cols = [col for col in date_cols
                          if last_month_start <= pd.to_datetime(col) <= last_month_end]
        
        # Calculate 95th percentile for each period
        if commissioning_cols and last_month_cols:
            commissioning_values = [row[col] for col in commissioning_cols if pd.notna(row[col]) and row[col] > 0]
            last_month_values = [row[col] for col in last_month_cols if pd.notna(row[col]) and row[col] > 0]
            
            if commissioning_values and last_month_values:
                initial_95th = np.percentile(commissioning_values, 95) / array_size
                latest_95th = np.percentile(last_month_values, 95) / array_size
                
                # Calculate years elapsed
                years_elapsed = (latest_date - first_date).days / 365.25
                
                # Calculate expected degradation
                if years_elapsed <= 1:
                    expected_degradation = years_elapsed * 1.5
                else:
                    expected_degradation = 1.5 + (years_elapsed - 1) * 0.4
                
                # Calculate actual degradation
                actual_degradation = ((initial_95th - latest_95th) / initial_95th * 100) if initial_95th > 0 else 0
                
                # Calculate performance vs expected
                performance_vs_expected = expected_degradation - actual_degradation
                
                # Check if site has data in last 3 days
                last_3_days = date_cols_sorted[:3] if len(date_cols_sorted) >= 3 else date_cols_sorted
                has_recent_data = any(pd.notna(row[date]) and row[date] > 0 for date in last_3_days)
                
                degradation_data.append({
                    'site_id': site_id,
                    'site_name': site_name_map.get(site_id, str(row['Site']) if pd.notna(row['Site']) else site_id),
                    'array_size': array_size,
                    'panel_description': str(row['Panel_Description']) if pd.notna(row['Panel_Description']) else 'N/A',
                    'province': row['Province_Full'],
                    'initial_yield_95th': initial_95th,
                    'latest_yield_95th': latest_95th,
                    'years_elapsed': years_elapsed,
                    'expected_degradation': expected_degradation,
                    'actual_degradation': actual_degradation,
                    'performance_vs_expected': performance_vs_expected,
                    'has_recent_data': has_recent_data,
                    'commissioned_date': first_date.strftime('%Y-%m-%d')
                })
    
    degradation_df = pd.DataFrame(degradation_data)
    print(f"  ✓ Degradation analysis complete for {len(degradation_df)} sites")
    
    # Prepare site data for JavaScript
    site_data = {}
    
    for idx, row in df.iterrows():
        site_id = row['Site_ID']
        
        # Extract daily data
        daily_data = []
        for date_col in date_cols:
            if pd.notna(row[date_col]):
                daily_data.append({
                    'date': date_col,
                    'solar_supply_kwh': float(row[date_col]),
                    'specific_yield': float(row[date_col]) / float(row['Array_Size_kWp']) if pd.notna(row['Array_Size_kWp']) and row['Array_Size_kWp'] > 0 else 0
                })
        
        # Helper function to safely convert to int
        def safe_int(value):
            try:
                return int(pd.to_numeric(value, errors='coerce')) if pd.notna(value) else 0
            except:
                return 0
        
        # Helper function to safely convert to float
        def safe_float(value):
            try:
                return float(pd.to_numeric(value, errors='coerce')) if pd.notna(value) else 0
            except:
                return 0
        
        site_data[site_id] = {
            'site_id': site_id,
            'site_name': site_name_map.get(site_id, str(row['Site']) if pd.notna(row['Site']) else site_id),
            'split': str(row['Split']) if pd.notna(row['Split']) else site_id,
            'po': str(row['PO']) if pd.notna(row['PO']) else 'N/A',
            'project': str(row['Project']) if pd.notna(row['Project']) else 'N/A',
            'grid_access': str(row['Grid Access']) if pd.notna(row['Grid Access']) else 'N/A',
            'power_sources': str(row['Power Sources']) if pd.notna(row['Power Sources']) else 'N/A',
            'panels': safe_int(row['Panels']),
            'panel_size': safe_int(row['Panel Size']),
            'panel_model': str(row['Panel Model']) if pd.notna(row['Panel Model']) else 'N/A',
            'panel_vendor': str(row['Panel Vendor']) if pd.notna(row['Panel Vendor']) else 'N/A',
            'panel_description': str(row['Panel_Description']) if pd.notna(row['Panel_Description']) else 'N/A',
            'array_size_kwp': safe_float(row['Array_Size_kWp']),
            'avg_load': safe_float(row['Avg Load']),
            'province': row['Province_Full'],
            'commissioned_date': site_commissioned_map.get(site_id, str(row['First_Production_Date']) if pd.notna(row['First_Production_Date']) else 'N/A'),
            'daily_data': daily_data,
            'prod_7d': safe_float(row['Prod_7d_kWh']),
            'avg_daily_7d': safe_float(row['Avg_Daily_7d_kWh']),
            'avg_yield_7d': safe_float(row['Avg_Yield_7d_kWh_kWp']),
            'prod_30d': safe_float(row['Prod_30d_kWh']),
            'avg_daily_30d': safe_float(row['Avg_Daily_30d_kWh']),
            'avg_yield_30d': safe_float(row['Avg_Yield_30d_kWh_kWp']),
            'prod_90d': safe_float(row['Prod_90d_kWh']),
            'avg_daily_90d': safe_float(row['Avg_Daily_90d_kWh']),
            'avg_yield_90d': safe_float(row['Avg_Yield_90d_kWh_kWp']),
            'total_production': safe_float(row['Total_Production_kWh']),
            'days_with_data': safe_int(row['Days_With_Data']),
            'avg_daily_all': safe_float(row['Avg_Daily_Production_kWh']),
            'avg_yield_all': safe_float(row['Avg_Specific_Yield_kWh_kWp_day']),
            'first_production_date': str(row['First_Production_Date']) if pd.notna(row['First_Production_Date']) else 'N/A'
        }
    
    # Calculate fleet statistics
    total_sites = len(df)
    sites_with_data = len(df[df['Days_With_Data'] > 0])
    total_capacity = df['Array_Size_kWp'].sum()
    
    # Calculate weighted average yields
    if total_capacity > 0:
        avg_yield_7d = (df['Avg_Yield_7d_kWh_kWp'] * df['Array_Size_kWp']).sum() / total_capacity
        avg_yield_30d = (df['Avg_Yield_30d_kWh_kWp'] * df['Array_Size_kWp']).sum() / total_capacity
        avg_yield_90d = (df['Avg_Yield_90d_kWh_kWp'] * df['Array_Size_kWp']).sum() / total_capacity
    else:
        avg_yield_7d = df['Avg_Yield_7d_kWh_kWp'].mean()
        avg_yield_30d = df['Avg_Yield_30d_kWh_kWp'].mean()
        avg_yield_90d = df['Avg_Yield_90d_kWh_kWp'].mean()
    
    # Calculate critical alerts (sites with 0 production in last 3 days)
    last_3_days = date_cols_sorted[:3] if len(date_cols_sorted) >= 3 else date_cols_sorted
    
    critical_alerts = []
    for idx, row in df.iterrows():
        has_zero_production = all(pd.isna(row[date]) or row[date] == 0 for date in last_3_days)
        if has_zero_production:
            critical_alerts.append(row['Site_ID'])
    
    # Categorize sites by performance
    excellent_sites_df = df[df['Avg_Yield_30d_kWh_kWp'] > 4.5]
    good_sites_df = df[(df['Avg_Yield_30d_kWh_kWp'] >= 3.5) & (df['Avg_Yield_30d_kWh_kWp'] <= 4.5)]
    fair_sites_df = df[(df['Avg_Yield_30d_kWh_kWp'] >= 2.5) & (df['Avg_Yield_30d_kWh_kWp'] < 3.5)]
    poor_sites_df = df[df['Avg_Yield_30d_kWh_kWp'] < 2.5]
    
    excellent_sites = excellent_sites_df.to_dict('records')
    good_sites = good_sites_df.to_dict('records')
    fair_sites = fair_sites_df.to_dict('records')
    poor_sites = poor_sites_df.to_dict('records')
    
    # Group by province
    province_stats = df.groupby('Province_Full').agg({
        'Site_ID': 'count',
        'Array_Size_kWp': 'sum',
        'Avg_Yield_30d_kWh_kWp': 'mean'
    }).reset_index()
    province_stats.columns = ['province', 'site_count', 'total_capacity', 'avg_yield']
    province_stats = province_stats.sort_values('avg_yield', ascending=False)
    
    # Group by project
    project_stats = df.groupby('Project').agg({
        'Site_ID': 'count',
        'Array_Size_kWp': 'sum',
        'Avg_Yield_30d_kWh_kWp': 'mean'
    }).reset_index()
    project_stats.columns = ['project', 'site_count', 'total_capacity', 'avg_yield']
    project_stats = project_stats.sort_values('avg_yield', ascending=False)
    
    # Group by panel type
    panel_stats = df.groupby('Panel_Description').agg({
        'Site_ID': 'count',
        'Array_Size_kWp': 'sum',
        'Avg_Yield_30d_kWh_kWp': 'mean'
    }).reset_index()
    panel_stats.columns = ['panel_type', 'site_count', 'total_capacity', 'avg_yield']
    panel_stats = panel_stats.sort_values('avg_yield', ascending=False)
    
    # Group by Grid Access
    grid_access_stats = df.groupby('Grid Access').agg({
        'Site_ID': 'count'
    }).reset_index()
    grid_access_stats.columns = ['grid_access', 'site_count']
    
    # Group by Power Sources
    power_sources_stats = df.groupby('Power Sources').agg({
        'Site_ID': 'count'
    }).reset_index()
    power_sources_stats.columns = ['power_sources', 'site_count']
    
    # Get commissioning timeline data with proper cumulative counts per date
    commissioning_timeline = df[df['First_Production_Date'].notna()].copy()
    commissioning_timeline['First_Production_Date'] = pd.to_datetime(commissioning_timeline['First_Production_Date'])
    commissioning_timeline = commissioning_timeline.sort_values('First_Production_Date')
    
    # Group by date and count sites commissioned on each date
    date_counts = commissioning_timeline.groupby('First_Production_Date').size().reset_index(name='count')
    date_counts = date_counts.sort_values('First_Production_Date')
    date_counts['cumulative_count'] = date_counts['count'].cumsum()
    
    # Convert dates to strings for JSON serialization
    commissioning_timeline_data = date_counts.copy()
    commissioning_timeline_data['First_Production_Date'] = commissioning_timeline_data['First_Production_Date'].dt.strftime('%Y-%m-%d')
    
    print(f"  Total Sites: {total_sites}")
    print(f"  Sites with Data: {sites_with_data}")
    print(f"  Total Capacity: {total_capacity:.1f} kWp")
    print(f"  Critical Alerts: {len(critical_alerts)}")
    print(f"  Provinces: {len(province_stats)}")
    print(f"  Projects: {len(project_stats)}")
    print(f"  Panel Types: {len(panel_stats)}")
    
    print(f"\n[4/4] Generating HTML dashboard...")
    
    # Generate HTML for site list items (not cards)
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
    
    # Generate province cards
    province_html = ''.join([
        f'''<div class="stat-card {'green' if p['avg_yield'] > 4.0 else 'yellow' if p['avg_yield'] > 3.0 else 'red'}">
            <div class="stat-label">{p['province']}</div>
            <div class="stat-value">{p['avg_yield']:.2f}</div>
            <div class="stat-subtitle">{int(p['site_count'])} sites • {p['total_capacity']:.1f} kWp</div>
        </div>'''
        for _, p in province_stats.iterrows()
    ])
    
    # Generate project cards
    project_html = ''.join([
        f'''<div class="stat-card {'green' if p['avg_yield'] > 4.0 else 'yellow' if p['avg_yield'] > 3.0 else 'red'}">
            <div class="stat-label">{p['project']}</div>
            <div class="stat-value">{p['avg_yield']:.2f}</div>
            <div class="stat-subtitle">{int(p['site_count'])} sites • {p['total_capacity']:.1f} kWp</div>
        </div>'''
        for _, p in project_stats.iterrows()
    ])
    
    # Generate panel type cards
    panel_html = ''.join([
        f'''<div class="stat-card {'green' if p['avg_yield'] > 4.0 else 'yellow' if p['avg_yield'] > 3.0 else 'red'}">
            <div class="stat-label">{p['panel_type']}</div>
            <div class="stat-value">{p['avg_yield']:.2f}</div>
            <div class="stat-subtitle">{int(p['site_count'])} sites • {p['total_capacity']:.1f} kWp</div>
        </div>'''
        for _, p in panel_stats.iterrows()
    ])
    
    # Get all site IDs for navigation
    all_site_ids = [str(site_id) for site_id in df['Site_ID'].tolist()]
    
    # Write HTML file
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Installed Sites Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif; background: #f8f9fa; color: #333; }}
        body.dark-mode {{ background: #1a1a1a; color: #e0e0e0; }}
        .header {{ background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); color: white; box-shadow: 0 4px 6px rgba(0,0,0,0.1); padding: 1.5rem 2rem; display: flex; align-items: center; justify-content: space-between; }}
        .dark-mode .header {{ background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%); }}
        .theme-toggle {{ background: rgba(255,255,255,0.2); border: 1px solid rgba(255,255,255,0.3); border-radius: 0.5rem; padding: 0.5rem 1rem; color: white; cursor: pointer; font-size: 0.875rem; transition: all 0.2s; }}
        .theme-toggle:hover {{ background: rgba(255,255,255,0.3); }}
        .header-content {{ display: flex; align-items: center; gap: 2rem; flex: 1; }}
        .header h1 {{ font-size: 2rem; font-weight: 600; margin: 0; }}
        .header p {{ opacity: 0.9; margin: 0; font-size: 1rem; }}
        .nav {{ background: #e9ecef; border-bottom: 1px solid #dee2e6; padding: 0 2rem; display: flex; gap: 2rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .dark-mode .nav {{ background: #2d3748; border-bottom: 1px solid #4a5568; }}
        .nav-item {{ padding: 1rem 0.5rem; cursor: pointer; border-bottom: 3px solid transparent; font-weight: 500; color: #6c757d; transition: all 0.2s; }}
        .nav-item:hover {{ color: #495057; }}
        .nav-item.active {{ color: #3498db; border-bottom-color: #3498db; }}
        .dark-mode .nav-item {{ color: #a0aec0; }}
        .dark-mode .nav-item.active {{ color: #4299e1; border-bottom-color: #4299e1; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 2rem; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1.5rem; margin-bottom: 2rem; }}
        .stat-card {{ background: white; border-radius: 0.75rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); padding: 1.5rem; border-left: 4px solid; transition: transform 0.2s, box-shadow 0.2s; }}
        .stat-card:hover {{ transform: translateY(-2px); box-shadow: 0 4px 6px rgba(0,0,0,0.15); }}
        .stat-card.blue {{ border-left-color: #3498db; }}
        .stat-card.green {{ border-left-color: #27ae60; }}
        .stat-card.yellow {{ border-left-color: #f39c12; }}
        .stat-card.red {{ border-left-color: #e74c3c; }}
        .stat-card.purple {{ border-left-color: #9b59b6; }}
        .dark-mode .stat-card {{ background: #2d3748; }}
        .stat-label {{ font-size: 0.875rem; font-weight: 600; color: #6c757d; text-transform: uppercase; letter-spacing: 0.05em; }}
        .dark-mode .stat-label {{ color: #a0aec0; }}
        .stat-value {{ font-size: 2.25rem; font-weight: bold; margin-top: 0.5rem; }}
        .stat-subtitle {{ font-size: 0.875rem; color: #6c757d; margin-top: 0.5rem; }}
        .dark-mode .stat-subtitle {{ color: #a0aec0; }}
        .chart-container {{ background: white; border-radius: 0.75rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); padding: 1.5rem; margin-bottom: 1.5rem; }}
        .chart-container h3 {{ font-size: 1.25rem; font-weight: bold; margin-bottom: 1rem; color: #333; }}
        .dark-mode .chart-container {{ background: #2d3748; }}
        .dark-mode .chart-container h3 {{ color: #e2e8f0; }}
        .hidden {{ display: none; }}
        .modal-overlay {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0, 0, 0, 0.7); z-index: 1000; overflow-y: auto; padding: 1rem; }}
        .modal-overlay.active {{ display: flex; align-items: flex-start; justify-content: center; padding-top: 2rem; }}
        .modal-content {{ background: white; border-radius: 0.75rem; max-width: 1400px; width: 100%; max-height: 95vh; overflow-y: auto; box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3); }}
        .dark-mode .modal-content {{ background: #2d3748; }}
        .modal-header {{ background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); color: white; padding: 1rem 1.5rem; border-radius: 0.75rem 0.75rem 0 0; position: sticky; top: 0; z-index: 10; }}
        .modal-header h2 {{ font-size: 1rem; margin: 0; font-weight: 600; }}
        .dark-mode .modal-header {{ background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%); }}
        .modal-close {{ position: absolute; top: 1rem; right: 1rem; background: rgba(255, 255, 255, 0.2); border: none; color: white; font-size: 1.5rem; width: 2.5rem; height: 2.5rem; border-radius: 50%; cursor: pointer; display: flex; align-items: center; justify-content: center; transition: all 0.2s; }}
        .modal-close:hover {{ background: rgba(255, 255, 255, 0.3); transform: rotate(90deg); }}
        .modal-body {{ padding: 1.5rem; }}
        .site-info-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 0.75rem; margin-bottom: 1rem; }}
        .site-info-item {{ background: #f8f9fa; padding: 0.75rem; border-radius: 0.5rem; border-left: 3px solid #3498db; }}
        .dark-mode .site-info-item {{ background: #1a365d; border-left-color: #2b6cb0; }}
        .site-info-label {{ font-size: 0.75rem; color: #6c757d; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.25rem; font-weight: 600; }}
        .dark-mode .site-info-label {{ color: #a0aec0; }}
        .site-info-value {{ font-size: 1.125rem; font-weight: 600; color: #333; }}
        .dark-mode .site-info-value {{ color: #e2e8f0; }}
        .period-stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 1.5rem; }}
        .period-card {{ background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); color: white; padding: 1rem; border-radius: 0.5rem; text-align: center; }}
        .period-card.green {{ background: linear-gradient(135deg, #27ae60 0%, #229954 100%); }}
        .period-card.yellow {{ background: linear-gradient(135deg, #f39c12 0%, #e67e22 100%); }}
        .period-card.red {{ background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); }}
        .period-label {{ font-size: 0.75rem; opacity: 0.9; margin-bottom: 0.5rem; font-weight: 600; }}
        .period-value {{ font-size: 1.5rem; font-weight: bold; }}
        .period-subtitle {{ font-size: 0.75rem; opacity: 0.8; margin-top: 0.25rem; }}
        .chart-wrapper {{ background: white; padding: 1rem; border-radius: 0.5rem; margin-bottom: 1rem; box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1); }}
        .chart-wrapper h4 {{ font-size: 1rem; margin-bottom: 0.75rem; color: #333; }}
        .dark-mode .chart-wrapper {{ background: #1a365d; }}
        .dark-mode .chart-wrapper h4 {{ color: #e2e8f0; }}
        .time-period-selector {{ display: flex; gap: 0.5rem; margin-bottom: 1rem; background: #e9ecef; padding: 0.375rem; border-radius: 0.5rem; }}
        .dark-mode .time-period-selector {{ background: #1a365d; }}
        .period-button {{ flex: 1; padding: 0.5rem 0.75rem; border: none; background: #ffffff; color: #495057; font-weight: 600; font-size: 0.875rem; border-radius: 0.375rem; cursor: pointer; transition: all 0.2s; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }}
        .period-button:hover {{ background: #f8f9fa; color: #3498db; transform: translateY(-1px); box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .period-button.active {{ background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); color: white; box-shadow: 0 2px 4px rgba(52, 152, 219, 0.3); }}
        .dark-mode .period-button {{ background: #2d3748; color: #a0aec0; }}
        .dark-mode .period-button:hover {{ background: #374151; color: #4299e1; }}
        .dark-mode .period-button.active {{ background: linear-gradient(135deg, #2b6cb0 0%, #2c5282 100%); color: white; }}
        .stats-summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 0.75rem; margin-bottom: 1rem; }}
        .summary-card {{ background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); color: white; padding: 0.75rem; border-radius: 0.5rem; text-align: center; }}
        .summary-card.green {{ background: linear-gradient(135deg, #27ae60 0%, #229954 100%); }}
        .summary-card.yellow {{ background: linear-gradient(135deg, #f39c12 0%, #e67e22 100%); }}
        .summary-card.red {{ background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); }}
        .summary-label {{ font-size: 0.7rem; opacity: 0.9; margin-bottom: 0.2rem; }}
        .summary-value {{ font-size: 1.25rem; font-weight: bold; }}
        .performance-section {{ border-radius: 0.75rem; padding: 1.5rem; margin-bottom: 1.5rem; transition: all 0.3s ease; }}
        .performance-section:hover {{ transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
        .performance-section.province {{ background: linear-gradient(135deg, rgba(52, 152, 219, 0.05) 0%, rgba(41, 128, 185, 0.08) 100%); border-left: 4px solid #3498db; }}
        .performance-section.project {{ background: linear-gradient(135deg, rgba(39, 174, 96, 0.05) 0%, rgba(34, 153, 84, 0.08) 100%); border-left: 4px solid #27ae60; }}
        .performance-section.panel {{ background: linear-gradient(135deg, rgba(243, 156, 18, 0.05) 0%, rgba(230, 126, 34, 0.08) 100%); border-left: 4px solid #f39c12; }}
        .dark-mode .performance-section.province {{ background: linear-gradient(135deg, rgba(52, 152, 219, 0.1) 0%, rgba(41, 128, 185, 0.15) 100%); }}
        .dark-mode .performance-section.project {{ background: linear-gradient(135deg, rgba(39, 174, 96, 0.1) 0%, rgba(34, 153, 84, 0.15) 100%); }}
        .dark-mode .performance-section.panel {{ background: linear-gradient(135deg, rgba(243, 156, 18, 0.1) 0%, rgba(230, 126, 34, 0.15) 100%); }}
    </style>
</head>
<body>
    <div class="header">
        <div class="header-content">
            <h1>Solar Performance Dashboard</h1>
            <p>Data from {total_sites} sites in the overview</p>
        </div>
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
                <div class="stat-card blue">
                    <div class="stat-label">Total Sites</div>
                    <div class="stat-value">{total_sites}</div>
                    <div class="stat-subtitle">{sites_with_data} with production data</div>
                </div>
                <div class="stat-card green">
                    <div class="stat-label">Total Capacity</div>
                    <div class="stat-value">{total_capacity:.1f}</div>
                    <div class="stat-subtitle">kWp installed capacity</div>
                </div>
                <div class="stat-card yellow">
                    <div class="stat-label">Avg Specific Yield</div>
                    <div class="stat-value">{avg_yield_30d:.2f}</div>
                    <div class="stat-subtitle">kWh/kWp/day (30-day avg)</div>
                </div>
                <div class="stat-card red">
                    <div class="stat-label">Critical Alerts</div>
                    <div class="stat-value">{len(critical_alerts)}</div>
                    <div class="stat-subtitle">Sites with 0 production (last 3 days)</div>
                </div>
            </div>
            
            <div class="chart-container">
                <h3>Performance Overview</h3>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin-top: 1rem;">
                    <div>
                        <h4 style="color: #27ae60; margin-bottom: 1rem; text-align: center;">📊 Performance Distribution</h4>
                        <div style="padding: 1rem; background: #f8f9fa; border-radius: 0.5rem;">
                            <div style="margin-bottom: 0.5rem;"><strong>Excellent (>4.5):</strong> {len(excellent_sites)} sites</div>
                            <div style="margin-bottom: 0.5rem;"><strong>Good (3.5-4.5):</strong> {len(good_sites)} sites</div>
                            <div style="margin-bottom: 0.5rem;"><strong>Fair (2.5-3.5):</strong> {len(fair_sites)} sites</div>
                            <div><strong>Poor (<2.5):</strong> {len(poor_sites)} sites</div>
                        </div>
                    </div>
                    <div>
                        <h4 style="color: #3498db; margin-bottom: 1rem; text-align: center;">📈 Fleet Health</h4>
                        <div style="padding: 1rem; background: #f8f9fa; border-radius: 0.5rem;">
                            <div style="margin-bottom: 0.5rem;"><strong>Sites Online:</strong> {sites_with_data} of {total_sites}</div>
                            <div style="margin-bottom: 0.5rem;"><strong>Last 7 Days Avg:</strong> {avg_yield_7d:.2f} kWh/kWp/day</div>
                            <div style="margin-bottom: 0.5rem;"><strong>Last 30 Days Avg:</strong> {avg_yield_30d:.2f} kWh/kWp/day</div>
                            <div><strong>Last 90 Days Avg:</strong> {avg_yield_90d:.2f} kWh/kWp/day</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="chart-container">
                <h3>Fleet Composition</h3>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin-top: 1rem;">
                    <div>
                        <h4 style="text-align: center; margin-bottom: 1rem;">Sites by Grid Access</h4>
                        <canvas id="gridAccessChart" style="max-height: 300px;"></canvas>
                    </div>
                    <div>
                        <h4 style="text-align: center; margin-bottom: 1rem;">Sites by Power Sources</h4>
                        <canvas id="powerSourcesChart" style="max-height: 300px;"></canvas>
                    </div>
                </div>
            </div>
            
            <div class="chart-container">
                <h3>Commissioning Timeline - Cumulative Sites Over Time</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Shows the total count of sites commissioned after each commissioning date</p>
                <canvas id="commissioningChart"></canvas>
            </div>
        </div>
        
        <div id="sites-tab" class="hidden">
            <div class="chart-container">
                <h3>🌟 Excellent Performance Sites (>4.5 kWh/kWp/day) - {len(excellent_sites)} sites</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Click any site to view detailed performance summary</p>
                <div style="max-height: 400px; overflow-y: auto;">{excellent_html}</div>
            </div>
            <div class="chart-container">
                <h3>✅ Good Performance Sites (3.5-4.5 kWh/kWp/day) - {len(good_sites)} sites</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Click any site to view detailed performance summary</p>
                <div style="max-height: 400px; overflow-y: auto;">{good_html}</div>
            </div>
            <div class="chart-container">
                <h3>⚠️ Fair Performance Sites (2.5-3.5 kWh/kWp/day) - {len(fair_sites)} sites</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Click any site to view detailed performance summary</p>
                <div style="max-height: 400px; overflow-y: auto;">{fair_html}</div>
            </div>
            <div class="chart-container">
                <h3>🚨 Poor Performance Sites (<2.5 kWh/kWp/day) - {len(poor_sites)} sites</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Click any site to view detailed performance summary</p>
                <div style="max-height: 400px; overflow-y: auto;">{poor_html}</div>
            </div>
        </div>
        
        <div id="degradation-tab" class="hidden">
            <div class="chart-container">
                <h3>🚨 Offline or No Data (Last 3 Days)</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Sites with no production data in the last 3 days - requires immediate attention</p>
                <div style="max-height: 400px; overflow-y: auto;" id="offline-sites-list"></div>
            </div>
            
            <div class="chart-container">
                <h3>🔴 High Degradation (>50%)</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Sites showing severe degradation above 50% - requires immediate attention and investigation</p>
                <div style="max-height: 400px; overflow-y: auto;" id="high-degradation-list"></div>
            </div>
            
            <div class="chart-container">
                <h3>⚠️ Medium Degradation (30-50%)</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Sites showing moderate degradation between 30-50% - monitor closely and plan maintenance</p>
                <div style="max-height: 400px; overflow-y: auto;" id="medium-degradation-list"></div>
            </div>
            
            <div class="chart-container">
                <h3>✅ Low Degradation (0-30%)</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Sites with acceptable degradation levels between 0-30% - normal performance range</p>
                <div style="max-height: 400px; overflow-y: auto;" id="low-degradation-list"></div>
            </div>
            
            <div class="chart-container">
                <h3>🌟 Better Than Expected (Negative degradation)</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Sites showing improvement over time - performing better than initial commissioning</p>
                <div style="max-height: 400px; overflow-y: auto;" id="better-degradation-list"></div>
            </div>
        </div>
        
        <div id="performance-tab" class="hidden">
            <div class="performance-section province">
                <h3>🏢 Province Performance</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Performance breakdown by geographical location - average specific yield per province</p>
                <div class="stats-grid">{province_html}</div>
            </div>
            
            <div class="performance-section project">
                <h3>📋 Project Performance</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Performance breakdown by project category - average specific yield per project type</p>
                <div class="stats-grid">{project_html}</div>
            </div>
            
            <div class="performance-section panel">
                <h3>⚡ Panel Type Performance</h3>
                <p style="font-size: 0.875rem; color: #6c757d; margin-bottom: 1rem;">Performance breakdown by solar panel technology - average specific yield per panel type</p>
                <div class="stats-grid">{panel_html}</div>
            </div>
        </div>
        
        <div id="site-modal" class="modal-overlay">
            <div class="modal-content">
                <div class="modal-header">
                    <h2 id="modal-site-name">Site Details</h2>
                    <button class="modal-close" onclick="closeSiteModal()">&times;</button>
                </div>
                <div class="modal-body" id="modal-body">
                    <div class="time-period-selector">
                        <button class="period-button" onclick="loadSiteData(this, '7d')">Last 7 Days</button>
                        <button class="period-button" onclick="loadSiteData(this, '30d')">Last 30 Days</button>
                        <button class="period-button active" onclick="loadSiteData(this, '90d')">Last 90 Days</button>
                        <button class="period-button" onclick="loadSiteData(this, 'all')">All Data</button>
                    </div>
                    <div class="site-info-grid" id="site-info-grid"></div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1rem;">
                        <div class="chart-wrapper">
                            <h4>Daily Production Trend</h4>
                            <canvas id="dailyProductionChart"></canvas>
                        </div>
                        <div class="chart-wrapper">
                            <h4>Specific Yield Trend</h4>
                            <canvas id="yieldTrendChart"></canvas>
                        </div>
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
    
    // Site category lists for navigation
    const excellentSiteIds = {json.dumps([str(site['Site_ID']) for site in excellent_sites])};
    const goodSiteIds = {json.dumps([str(site['Site_ID']) for site in good_sites])};
    const fairSiteIds = {json.dumps([str(site['Site_ID']) for site in fair_sites])};
    const poorSiteIds = {json.dumps([str(site['Site_ID']) for site in poor_sites])};
    
    // Degradation category lists for navigation
    const offlineSiteIds = [];
    const highDegradationIds = [];
    const mediumDegradationIds = [];
    const lowDegradationIds = [];
    const betterDegradationIds = [];
    
    let currentSiteId = null;
    let currentSiteIndex = 0;
    let currentSiteList = [];
    let currentCategory = 'all'; // Track which category we're navigating within
    let siteCharts = [];
    let currentPeriod = '90d';  // Track current period selection
    
    function showTab(element, tabName) {{
        document.querySelectorAll(".nav-item").forEach(item => item.classList.remove("active"));
        element.classList.add("active");
        document.querySelectorAll("[id$='-tab']").forEach(tab => tab.classList.add("hidden"));
        document.getElementById(tabName + "-tab").classList.remove("hidden");
    }}
    
    function toggleTheme() {{
        document.body.classList.toggle("dark-mode");
        const button = document.querySelector(".theme-toggle");
        button.textContent = document.body.classList.contains("dark-mode") ? "☀️ Light Mode" : "🌙 Dark Mode";
    }}
    
    function openSiteModal(siteId, category) {{
        currentSiteId = siteId;
        currentCategory = category || 'all';
        
        // Set the appropriate site list based on category
        switch(currentCategory) {{
            case 'excellent':
                currentSiteList = excellentSiteIds;
                break;
            case 'good':
                currentSiteList = goodSiteIds;
                break;
            case 'fair':
                currentSiteList = fairSiteIds;
                break;
            case 'poor':
                currentSiteList = poorSiteIds;
                break;
            case 'offline':
                currentSiteList = offlineSiteIds;
                break;
            case 'high-degradation':
                currentSiteList = highDegradationIds;
                break;
            case 'medium-degradation':
                currentSiteList = mediumDegradationIds;
                break;
            case 'low-degradation':
                currentSiteList = lowDegradationIds;
                break;
            case 'better-degradation':
                currentSiteList = betterDegradationIds;
                break;
            default:
                currentSiteList = allSiteIds;
        }}
        
        currentSiteIndex = currentSiteList.indexOf(siteId);
        
        const modal = document.getElementById("site-modal");
        const site = siteData[siteId];
        
        if (!site) {{
            alert("Site data not available");
            return;
        }}
        
        const prevDisabled = currentSiteIndex <= 0 ? 'disabled' : '';
        const nextDisabled = currentSiteIndex >= currentSiteList.length - 1 ? 'disabled' : '';
        const prevStyle = currentSiteIndex <= 0 ? 'opacity: 0.3; cursor: not-allowed;' : 'cursor: pointer;';
        const nextStyle = currentSiteIndex >= currentSiteList.length - 1 ? 'opacity: 0.3; cursor: not-allowed;' : 'cursor: pointer;';
        
        document.getElementById("modal-site-name").innerHTML = `
            <div style="display: flex; align-items: center; width: 100%;">
                <div style="display: flex; gap: 0.5rem; align-items: center;">
                    <button onclick="navigateSite(-1)" style="background: rgba(255,255,255,0.2); border: none; color: white; padding: 0.3rem; border-radius: 50%; font-size: 0.9rem; width: 1.8rem; height: 1.8rem; ${{prevStyle}}" ${{prevDisabled}}>‹</button>
                    <button onclick="navigateSite(1)" style="background: rgba(255,255,255,0.2); border: none; color: white; padding: 0.3rem; border-radius: 50%; font-size: 0.9rem; width: 1.8rem; height: 1.8rem; ${{nextStyle}}" ${{nextDisabled}}>›</button>
                </div>
                <div style="flex: 1; text-align: center; margin-left: 1rem;">
                    <div style="font-size: 0.95rem; font-weight: 600;">${{site.site_name}}</div>
                    <div style="font-size: 0.75rem; opacity: 0.9; margin-top: 0.15rem;">${{site.panel_description}} • ${{site.project}}</div>
                </div>
            </div>
        `;
        
        // Restore the previously selected period
        document.querySelectorAll(".period-button").forEach(btn => btn.classList.remove("active"));
        const periodButtons = document.querySelectorAll(".period-button");
        const periodMap = {{'7d': 0, '30d': 1, '90d': 2, 'all': 3}};
        const buttonIndex = periodMap[currentPeriod] || 2;
        if (periodButtons[buttonIndex]) {{
            periodButtons[buttonIndex].classList.add("active");
            loadSiteData(periodButtons[buttonIndex], currentPeriod);
        }}
        
        modal.classList.add("active");
    }}
    
    function navigateSite(direction) {{
        const newIndex = currentSiteIndex + direction;
        if (newIndex >= 0 && newIndex < currentSiteList.length) {{
            currentSiteIndex = newIndex;
            const newSiteId = currentSiteList[newIndex];
            openSiteModal(newSiteId, currentCategory);
        }}
    }}
    
    function closeSiteModal() {{
        document.getElementById("site-modal").classList.remove("active");
        if (siteCharts.length > 0) {{
            siteCharts.forEach(chart => chart.destroy());
            siteCharts = [];
        }}
    }}
    
    function loadSiteData(button, period) {{
        currentPeriod = period;  // Save current period selection
        document.querySelectorAll(".period-button").forEach(btn => btn.classList.remove("active"));
        button.classList.add("active");
        
        if (!currentSiteId) return;
        const site = siteData[currentSiteId];
        if (!site || !site.daily_data) return;
        
        const data = site.daily_data;
        let filteredData = data;
        const now = new Date();
        const days = {{"7d": 7, "30d": 30, "90d": 90}};
        
        if (period !== "all") {{
            const cutoff = new Date(now - days[period] * 24 * 60 * 60 * 1000);
            filteredData = data.filter(d => new Date(d.date) >= cutoff);
        }}
        
        const validData = filteredData.filter(d => !isNaN(d.solar_supply_kwh) && !isNaN(d.specific_yield));
        
        const totalProd = validData.reduce((sum, d) => sum + (d.solar_supply_kwh || 0), 0);
        const avgYield = validData.length > 0 ? validData.reduce((sum, d) => sum + (d.specific_yield || 0), 0) / validData.length : 0;
        const maxProd = validData.length > 0 ? Math.max(...validData.map(d => d.solar_supply_kwh || 0)) : 0;
        const minProd = validData.length > 0 ? Math.min(...validData.map(d => d.solar_supply_kwh || 0)) : 0;
        
        document.getElementById("site-info-grid").innerHTML = `
            <div class="site-info-item">
                <div class="site-info-label">Panel Type</div>
                <div class="site-info-value">${{site.panel_description}}</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Array Size</div>
                <div class="site-info-value">${{site.array_size_kwp.toFixed(2)}} kWp</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Avg Load</div>
                <div class="site-info-value">${{site.avg_load.toFixed(1)}} kW</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Grid Access</div>
                <div class="site-info-value">${{site.grid_access}}</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Power Sources</div>
                <div class="site-info-value">${{site.power_sources}}</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Project</div>
                <div class="site-info-value">${{site.project}}</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">PO Number</div>
                <div class="site-info-value">${{site.po}}</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Province</div>
                <div class="site-info-value">${{site.province}}</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Commissioning</div>
                <div class="site-info-value">${{site.commissioned_date}}</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Panel Vendor</div>
                <div class="site-info-value">${{site.panel_vendor}}</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Panel Model</div>
                <div class="site-info-value">${{site.panel_model}}</div>
            </div>
            <div class="site-info-item">
                <div class="site-info-label">Panels Count</div>
                <div class="site-info-value">${{site.panels}} × ${{site.panel_size}}W</div>
            </div>
        `;
        
        document.getElementById("site-stats-summary").innerHTML = `
            <div class="summary-card">
                <div class="summary-label">Total Production</div>
                <div class="summary-value">${{totalProd.toFixed(1)}} kWh</div>
            </div>
            <div class="summary-card green">
                <div class="summary-label">Average Yield</div>
                <div class="summary-value">${{avgYield.toFixed(2)}} kWh/kWp</div>
            </div>
            <div class="summary-card yellow">
                <div class="summary-label">Peak Production</div>
                <div class="summary-value">${{maxProd.toFixed(1)}} kWh</div>
            </div>
            <div class="summary-card red">
                <div class="summary-label">Low Production</div>
                <div class="summary-value">${{minProd.toFixed(1)}} kWh</div>
            </div>
        `;
        
        if (siteCharts.length > 0) {{
            siteCharts.forEach(chart => chart.destroy());
            siteCharts = [];
        }}
        
        const dailyCtx = document.getElementById("dailyProductionChart").getContext("2d");
        siteCharts.push(new Chart(dailyCtx, {{
            type: "line",
            data: {{
                labels: validData.map(d => new Date(d.date).toLocaleDateString()),
                datasets: [{{
                    label: "Production (kWh)",
                    data: validData.map(d => d.solar_supply_kwh || 0),
                    borderColor: "#3498db",
                    backgroundColor: "rgba(52, 152, 219, 0.1)",
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }}));
        
        const yieldCtx = document.getElementById("yieldTrendChart").getContext("2d");
        siteCharts.push(new Chart(yieldCtx, {{
            type: "line",
            data: {{
                labels: validData.map(d => new Date(d.date).toLocaleDateString()),
                datasets: [{{
                    label: "Specific Yield (kWh/kWp)",
                    data: validData.map(d => d.specific_yield || 0),
                    borderColor: "#27ae60",
                    backgroundColor: "rgba(39, 174, 96, 0.1)",
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }}));
    }}
    
    // Initialize charts on page load
    function initializeGridAccessChart() {{
        const canvas = document.getElementById("gridAccessChart");
        if (!canvas) return;
        const ctx = canvas.getContext("2d");
        
        const colors = ['#3498db', '#27ae60', '#f39c12', '#e74c3c', '#9b59b6'];
        
        new Chart(ctx, {{
            type: "pie",
            data: {{
                labels: gridAccessData.map(d => d.grid_access),
                datasets: [{{
                    data: gridAccessData.map(d => d.site_count),
                    backgroundColor: colors.slice(0, gridAccessData.length),
                    borderColor: '#fff',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom'
                    }}
                }}
            }}
        }});
    }}
    
    function initializePowerSourcesChart() {{
        const canvas = document.getElementById("powerSourcesChart");
        if (!canvas) return;
        const ctx = canvas.getContext("2d");
        
        const colors = ['#e74c3c', '#3498db', '#27ae60', '#f39c12', '#9b59b6'];
        
        new Chart(ctx, {{
            type: "pie",
            data: {{
                labels: powerSourcesData.map(d => d.power_sources),
                datasets: [{{
                    data: powerSourcesData.map(d => d.site_count),
                    backgroundColor: colors.slice(0, powerSourcesData.length),
                    borderColor: '#fff',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom'
                    }}
                }}
            }}
        }});
    }}
    
    function initializeCommissioningChart() {{
        const canvas = document.getElementById("commissioningChart");
        if (!canvas || commissioningData.length === 0) return;
        const ctx = canvas.getContext("2d");
        
        new Chart(ctx, {{
            type: "line",
            data: {{
                labels: commissioningData.map(d => new Date(d.First_Production_Date).toLocaleDateString()),
                datasets: [{{
                    label: "Cumulative Sites Commissioned",
                    data: commissioningData.map(d => d.cumulative_count),
                    borderColor: "#3498db",
                    backgroundColor: "rgba(52, 152, 219, 0.1)",
                    fill: true,
                    tension: 0,
                    stepped: false,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    pointBackgroundColor: "#3498db",
                    pointBorderColor: "#fff",
                    pointBorderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            stepSize: 1,
                            precision: 0
                        }},
                        title: {{
                            display: true,
                            text: 'Total Number of Sites Commissioned',
                            font: {{
                                size: 14,
                                weight: 'bold'
                            }}
                        }}
                    }},
                    x: {{
                        title: {{
                            display: true,
                            text: 'Commissioning Date',
                            font: {{
                                size: 14,
                                weight: 'bold'
                            }}
                        }}
                    }}
                }},
                plugins: {{
                    legend: {{
                        display: true,
                        position: 'top'
                    }},
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                return 'Date: ' + context[0].label;
                            }},
                            label: function(context) {{
                                const dataIndex = context.dataIndex;
                                const sitesOnDate = commissioningData[dataIndex].count;
                                const totalSites = context.parsed.y;
                                return [
                                    'Sites commissioned on this date: ' + sitesOnDate,
                                    'Total sites commissioned: ' + totalSites
                                ];
                            }}
                        }}
                    }}
                }}
            }}
        }});
    }}
    
    // Initialize degradation lists
    function initializeDegradationLists() {{
        if (!degradationData || degradationData.length === 0) {{
            return;
        }}
        
        // Separate sites by category with new thresholds
        const offlineSites = degradationData.filter(s => !s.has_recent_data);
        const onlineSites = degradationData.filter(s => s.has_recent_data);
        
        // New degradation categories based on actual degradation percentage
        const highDeg = onlineSites.filter(s => s.actual_degradation > 50).sort((a, b) => b.actual_degradation - a.actual_degradation);
        const mediumDeg = onlineSites.filter(s => s.actual_degradation >= 30 && s.actual_degradation <= 50).sort((a, b) => b.actual_degradation - a.actual_degradation);
        const lowDeg = onlineSites.filter(s => s.actual_degradation >= 0 && s.actual_degradation < 30).sort((a, b) => b.actual_degradation - a.actual_degradation);
        const betterDeg = onlineSites.filter(s => s.actual_degradation < 0).sort((a, b) => a.actual_degradation - b.actual_degradation);
        
        // Generate HTML for each category
        function generateDegradationItem(site, color, category) {{
            const colorMap = {{'green': '#27ae60', 'blue': '#3498db', 'yellow': '#f39c12', 'red': '#e74c3c'}};
            const degradationText = site.actual_degradation >= 0
                ? `${{site.actual_degradation.toFixed(1)}}% degradation`
                : `${{Math.abs(site.actual_degradation).toFixed(1)}}% improvement`;
            const expectedText = `Expected: ${{site.expected_degradation.toFixed(1)}}%`;
            const performanceText = site.performance_vs_expected >= 0
                ? `${{site.performance_vs_expected.toFixed(1)}}% better than expected`
                : `${{Math.abs(site.performance_vs_expected).toFixed(1)}}% worse than expected`;
            
            return `<div class="site-list-item" onclick="openSiteModal('${{site.site_id}}', '${{category}}')" style="cursor: pointer; padding: 0.75rem; border-left: 3px solid ${{colorMap[color]}}; margin-bottom: 0.5rem; background: #f8f9fa; border-radius: 0.5rem; transition: transform 0.2s;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="font-weight: 600;">${{site.site_name}}</div>
                    <div style="font-weight: bold; color: ${{colorMap[color]}};">${{degradationText}}</div>
                </div>
                <div style="font-size: 0.875rem; color: #6c757d; margin-top: 0.25rem;">
                    ${{site.panel_description}} • ${{site.array_size.toFixed(1)}} kWp • ${{site.years_elapsed.toFixed(1)}} years old
                </div>
                <div style="font-size: 0.75rem; color: #495057; margin-top: 0.25rem;">
                    Initial: ${{site.initial_yield_95th.toFixed(2)}} kWh/kWp → Latest: ${{site.latest_yield_95th.toFixed(2)}} kWh/kWp | ${{expectedText}} | ${{performanceText}}
                </div>
            </div>`;
        }}
        
        // Populate site ID arrays for navigation
        offlineSiteIds.length = 0;
        offlineSiteIds.push(...offlineSites.map(s => s.site_id));
        
        highDegradationIds.length = 0;
        highDegradationIds.push(...highDeg.map(s => s.site_id));
        
        mediumDegradationIds.length = 0;
        mediumDegradationIds.push(...mediumDeg.map(s => s.site_id));
        
        lowDegradationIds.length = 0;
        lowDegradationIds.push(...lowDeg.map(s => s.site_id));
        
        betterDegradationIds.length = 0;
        betterDegradationIds.push(...betterDeg.map(s => s.site_id));
        
        // Populate offline sites
        const offlineHtml = offlineSites.length > 0
            ? offlineSites.map(s => generateDegradationItem(s, 'red', 'offline')).join('')
            : '<p style="color: #6c757d; padding: 1rem;">No offline sites detected</p>';
        document.getElementById('offline-sites-list').innerHTML = offlineHtml;
        
        // Populate high degradation
        const highHtml = highDeg.length > 0
            ? highDeg.map(s => generateDegradationItem(s, 'red', 'high-degradation')).join('')
            : '<p style="color: #6c757d; padding: 1rem;">No sites in this category</p>';
        document.getElementById('high-degradation-list').innerHTML = highHtml;
        
        // Populate medium degradation
        const mediumHtml = mediumDeg.length > 0
            ? mediumDeg.map(s => generateDegradationItem(s, 'yellow', 'medium-degradation')).join('')
            : '<p style="color: #6c757d; padding: 1rem;">No sites in this category</p>';
        document.getElementById('medium-degradation-list').innerHTML = mediumHtml;
        
        // Populate low degradation
        const lowHtml = lowDeg.length > 0
            ? lowDeg.map(s => generateDegradationItem(s, 'blue', 'low-degradation')).join('')
            : '<p style="color: #6c757d; padding: 1rem;">No sites in this category</p>';
        document.getElementById('low-degradation-list').innerHTML = lowHtml;
        
        // Populate better than expected
        const betterHtml = betterDeg.length > 0
            ? betterDeg.map(s => generateDegradationItem(s, 'green', 'better-degradation')).join('')
            : '<p style="color: #6c757d; padding: 1rem;">No sites in this category</p>';
        document.getElementById('better-degradation-list').innerHTML = betterHtml;
    }}
    
    // Initialize charts when page loads
    document.addEventListener("DOMContentLoaded", function() {{
        initializeGridAccessChart();
        initializePowerSourcesChart();
        initializeCommissioningChart();
        initializeDegradationLists();
    }});
    
    // Close modal on outside click
    document.getElementById("site-modal").addEventListener("click", function(e) {{
        if (e.target === this) {{
            closeSiteModal();
        }}
    }});
    </script>
</body>
</html>"""
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"✓ Dashboard generated successfully!")
        print(f"  Output file: {output_file}")
        print(f"\n{'='*70}")
        print("COMPLETE! Open installed_sites_dashboard.html to view the dashboard.")
        print(f"{'='*70}")
    except Exception as e:
        print(f"✗ Error writing HTML file: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    generate_installed_sites_dashboard()