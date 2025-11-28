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
    print("FULL-FEATURED MOBILE GENERATOR (OPTIMIZED)")
    print("="*70)
    
    scripts_folder = Path(__file__).parent.resolve()
    output_dir = scripts_folder / OUTPUT_FOLDER
    data_dir = output_dir / "site_data"
    
    # 1. Setup Folders
    if output_dir.exists(): shutil.rmtree(output_dir)
    output_dir.mkdir()
    data_dir.mkdir()

    # 2. Load Data
    excel_files = list(scripts_folder.glob("installed_sites_production_*.xlsx"))
    if not excel_files: return print("✗ No production file found.")
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
    except: pass

    # 4. Pre-process Columns
    def safe_get(row, key, default=0, type_func=float):
        val = row.get(key, default)
        try: return type_func(val) if pd.notna(val) else default
        except: return default

    df['Province_Full'] = df['Site_ID'].astype(str).str[:2].apply(get_province_full_name)
    date_cols = sorted([c for c in df.columns if isinstance(c, str) and len(c)==10 and c[4]=='-'], reverse=True)
    col_to_date = {c: pd.to_datetime(c) for c in date_cols}
    latest_date = col_to_date[date_cols[0]] if date_cols else datetime.now()

    # 5. Global Stats Containers
    site_metadata = {}
    
    # Aggregators
    fleet_stats = {
        'total_sites': len(df),
        'online_sites': 0,
        'capacity': df['Array_Size_kWp'].sum(),
        'avg_yield_30d': df['Avg_Yield_30d_kWh_kWp'].mean(),
        'critical_alerts': 0,
        'perf_dist': {'Excellent':0, 'Good':0, 'Fair':0, 'Poor':0}
    }
    
    chart_data = {
        'grid_access': df['Grid Access'].fillna('Unknown').value_counts().to_dict(),
        'power_sources': df['Power Sources'].fillna('Unknown').value_counts().to_dict(),
        'commissioning': {}
    }

    # Timeline Logic
    comm_dates = pd.to_datetime(df['First_Production_Date'], errors='coerce').dropna().sort_values()
    chart_data['commissioning'] = comm_dates.groupby(comm_dates).size().cumsum().to_dict()
    chart_data['commissioning'] = {k.strftime('%Y-%m-%d'): v for k,v in chart_data['commissioning'].items()}

    print(f"  Processing {len(df)} sites...")

    for _, row in df.iterrows():
        sid = str(row['Site_ID'])
        size = safe_get(row, 'Array_Size_kWp')
        if size <= 0: continue

        # Determine Panel Description (Logic from Dashboard Generator)
        panel_desc = str(row.get('Panel_Description', ''))
        if panel_desc == 'nan' or not panel_desc:
            p_size = str(int(safe_get(row, 'Panel Size'))) if safe_get(row, 'Panel Size') > 0 else 'Unknown'
            p_vend = str(row.get('Panel Vendor', 'Unknown'))
            panel_desc = f"{p_size} {p_vend}"

        # Basic Info
        meta = {
            'id': sid,
            'name': site_db_info.get(sid, {}).get('site_name', str(row.get('Site', sid))),
            'prov': row['Province_Full'],
            'kwp': round(size, 2),
            'yld': round(safe_get(row, 'Avg_Yield_30d_kWh_kWp'), 2),
            'panel': panel_desc,
            'proj': str(row.get('Project', 'N/A')),
            'grid': str(row.get('Grid Access', 'N/A')),
            'src': str(row.get('Power Sources', 'N/A')),
            'comm': str(row.get('First_Production_Date', 'N/A')),
            
            # Default Degradation Values
            'deg_cat': 'Unknown',
            'deg_act': 0,
            'deg_exp': 0,
            'perf_vs_exp': 0,
            'online': False,
            
            # Additional Stats needed for lists
            'years': 0
        }
        
        # Performance Category
        if meta['yld'] > 4.5: cat = 'Excellent'
        elif meta['yld'] >= 3.5: cat = 'Good'
        elif meta['yld'] >= 2.5: cat = 'Fair'
        else: cat = 'Poor'
        meta['cat'] = cat
        fleet_stats['perf_dist'][cat] += 1

        # Check Offline (Last 3 days 0) - Matches dashboard_generator logic
        recent_cols = date_cols[:3] if len(date_cols) >= 3 else date_cols
        is_online = any(safe_get(row, d) > 0 for d in recent_cols)
        meta['online'] = is_online
        
        if is_online: 
            fleet_stats['online_sites'] += 1
        else: 
            fleet_stats['critical_alerts'] += 1
            meta['deg_cat'] = 'Offline'

        # DEGRADATION CALCULATION (Matches Dashboard Generator Math)
        if is_online and pd.notna(row['First_Production_Date']):
            try:
                first_date = pd.to_datetime(row['First_Production_Date'])
                comm_end = first_date + pd.DateOffset(months=1)
                last_start = latest_date - pd.DateOffset(months=1)
                
                def get_vals(start, end):
                    cols = [c for c in date_cols if start <= col_to_date[c] <= end]
                    return [row[c] for c in cols if pd.notna(row[c]) and row[c] > 0]

                c_vals = get_vals(first_date, comm_end)
                l_vals = get_vals(last_start, latest_date)

                if c_vals and l_vals:
                    init_95 = np.percentile(c_vals, 95) / size
                    curr_95 = np.percentile(l_vals, 95) / size
                    
                    years = (latest_date - first_date).days / 365.25
                    meta['years'] = round(years, 1)
                    
                    # Exact formula from dashboard_generator
                    if years <= 1:
                        expected = years * 3
                    else:
                        expected = 3 + (years - 1) * 0.7
                        
                    actual = ((init_95 - curr_95) / init_95 * 100) if init_95 > 0 else 0
                    perf_vs = expected - actual
                    
                    meta['deg_act'] = round(actual, 1)
                    meta['deg_exp'] = round(expected, 1)
                    meta['perf_vs_exp'] = round(perf_vs, 1)
                    meta['init_yield'] = round(init_95, 2)
                    meta['curr_yield'] = round(curr_95, 2)
                    
                    if actual > 50: meta['deg_cat'] = 'High'
                    elif actual >= 30: meta['deg_cat'] = 'Medium'
                    elif actual >= 0: meta['deg_cat'] = 'Low'
                    else: meta['deg_cat'] = 'Better'
            except:
                pass

        site_metadata[sid] = meta

        # HEAVY DATA -> SEPARATE JSON FILE (Optimization)
        daily_hist = []
        for d in date_cols:
            if pd.notna(row[d]):
                val = float(row[d])
                daily_hist.append({
                    'd': d, 
                    'v': val, 
                    'y': round(val/size, 2) if size else 0
                })
        
        # Only save last 365 days to save space on mobile
        daily_hist = daily_hist[:365] 
        
        with open(data_dir / f"{sid}.json", 'w') as f:
            json.dump({'meta': meta, 'hist': daily_hist}, f)

    # 6. Aggregates for Categorization views
    provinces = df.groupby('Province_Full')['Avg_Yield_30d_kWh_kWp'].mean().to_dict()
    projects = df.groupby('Project')['Avg_Yield_30d_kWh_kWp'].mean().to_dict()
    # ADDED: Panel grouping to match PC dashboard
    panels = df.groupby('Panel_Description')['Avg_Yield_30d_kWh_kWp'].mean().to_dict()
    
    # 7. Generate HTML
    print("  Generating Mobile HTML...")
    
    # Embed the LIGHTWEIGHT metadata, not the heavy history
    json_metadata = json.dumps(site_metadata)
    json_charts = json.dumps(chart_data)
    json_provs = json.dumps(provinces)
    json_projs = json.dumps(projects)
    json_panels = json.dumps(panels)
    json_dist = json.dumps(fleet_stats['perf_dist'])
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Solar Fleet Mobile</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{ --bg: #f4f6f8; --card: #fff; --text: #333; --blue: #3498db; --green: #27ae60; --red: #e74c3c; --yellow: #f39c12; }}
        [data-theme="dark"] {{ --bg: #1a1a1a; --card: #2d3748; --text: #e0e0e0; }}
        
        body {{ font-family: -apple-system, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding-bottom: 80px; -webkit-tap-highlight-color: transparent; }}
        
        /* Navigation */
        .header {{ background: linear-gradient(135deg, #2c3e50, #3498db); color: white; padding: 1rem; position: sticky; top: 0; z-index: 50; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header-top {{ display: flex; justify-content: space-between; align-items: center; }}
        .tabs {{ display: flex; overflow-x: auto; background: var(--card); padding: 0.5rem; gap: 0.5rem; border-bottom: 1px solid #ddd; position: sticky; top: 60px; z-index: 40; scrollbar-width: none; }}
        .tabs::-webkit-scrollbar {{ display: none; }}
        .tab {{ padding: 8px 16px; border-radius: 20px; white-space: nowrap; cursor: pointer; background: var(--bg); color: var(--text); font-weight: 500; font-size: 0.9rem; border: 1px solid transparent; }}
        .tab.active {{ background: var(--blue); color: white; }}
        
        /* Layout */
        .page {{ display: none; padding: 1rem; animation: fadeIn 0.3s; }}
        .page.active {{ display: block; }}
        @keyframes fadeIn {{ from {{ opacity:0 }} to {{ opacity:1 }} }}
        
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 1rem; margin-bottom: 1rem; }}
        .card {{ background: var(--card); padding: 1rem; border-radius: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
        .big-num {{ font-size: 1.8rem; font-weight: bold; margin: 0.5rem 0; }}
        
        /* Site List Items */
        .site-item {{ background: var(--card); padding: 1rem; margin-bottom: 0.5rem; border-radius: 12px; border-left: 5px solid #ccc; cursor: pointer; transition: transform 0.1s; position: relative; }}
        .site-item:active {{ transform: scale(0.98); }}
        .badg {{ padding: 4px 8px; border-radius: 6px; font-size: 0.8rem; color: white; float: right; font-weight: bold; margin-left: 10px; }}
        
        /* Modal */
        .modal {{ display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.85); z-index: 100; overflow-y: auto; }}
        .modal.open {{ display: block; }}
        .modal-content {{ background: var(--card); margin: 1rem auto; width: 95%; max-width: 800px; border-radius: 16px; overflow: hidden; min-height: 50vh; }}
        
        /* Colors */
        .c-exc {{ border-color: var(--green) !important; }} .bg-exc {{ background: var(--green); }}
        .c-good {{ border-color: var(--blue) !important; }} .bg-good {{ background: var(--blue); }}
        .c-fair {{ border-color: var(--yellow) !important; }} .bg-fair {{ background: var(--yellow); }}
        .c-poor {{ border-color: var(--red) !important; }} .bg-poor {{ background: var(--red); }}
        
        .deg-btn {{ flex: 1; text-align: center; font-size: 0.8rem; padding: 8px 4px; }}
        .search-bar {{ width:100%; padding:14px; border-radius:12px; border:1px solid #ccc; margin-bottom:1rem; font-size:16px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); box-sizing:border-box; background: var(--card); color: var(--text); }}
        
        .cat-header {{ margin-top: 1.5rem; margin-bottom: 0.5rem; font-size: 1.1rem; color: var(--blue); border-bottom: 2px solid var(--blue); display: inline-block; padding-bottom: 4px; }}
    </style>
</head>
<body>

<div class="header">
    <div class="header-top">
        <div>
            <h2 style="margin:0; font-size: 1.4rem;">Solar Fleet</h2>
            <div style="font-size:0.8rem; opacity:0.8; margin-top:2px">{fleet_stats['total_sites']} Sites • {int(fleet_stats['capacity'])} kWp</div>
        </div>
        <button onclick="toggleTheme()" style="background:rgba(255,255,255,0.2); border:none; color:white; padding:8px 12px; border-radius:8px; font-size: 1.2rem;">🌙</button>
    </div>
</div>

<div class="tabs">
    <div class="tab active" onclick="nav('overview', this)">Overview</div>
    <div class="tab" onclick="nav('sites', this)">All Sites</div>
    <div class="tab" onclick="nav('degradation', this)">Degradation</div>
    <div class="tab" onclick="nav('perf', this)">Categories</div>
</div>

<div id="overview" class="page active">
    <div class="grid">
        <div class="card" style="border-left: 4px solid var(--blue)">
            <small>Avg Yield (30d)</small>
            <div class="big-num">{fleet_stats['avg_yield_30d']:.2f}</div>
        </div>
        <div class="card" style="border-left: 4px solid var(--red)">
            <small>Critical Alerts</small>
            <div class="big-num">{fleet_stats['critical_alerts']}</div>
        </div>
        <div class="card" style="border-left: 4px solid var(--green)">
             <small>Sites Online</small>
             <div class="big-num">{fleet_stats['online_sites']} / {fleet_stats['total_sites']}</div>
        </div>
    </div>
    
    <div class="card"><h3>Performance Distribution</h3><canvas id="distChart" height="200"></canvas></div>
    <div class="card" style="margin-top:1rem"><h3>Commissioning Timeline</h3><canvas id="commChart" height="200"></canvas></div>
    
    <div class="grid" style="margin-top:1rem">
        <div class="card"><h4>Grid Access</h4><canvas id="gridChart" height="150"></canvas></div>
        <div class="card"><h4>Power Sources</h4><canvas id="powerChart" height="150"></canvas></div>
    </div>
</div>

<div id="sites" class="page">
    <input type="text" class="search-bar" placeholder="Search site, province, panel type..." onkeyup="renderSites(this.value)">
    
    <div style="display:flex; gap:0.5rem; overflow-x:auto; margin-bottom:1rem; padding-bottom:5px;">
        <button class="tab deg-btn active" onclick="filterSites('All', this)">All</button>
        <button class="tab deg-btn" onclick="filterSites('Excellent', this)">Exc (>4.5)</button>
        <button class="tab deg-btn" onclick="filterSites('Good', this)">Good</button>
        <button class="tab deg-btn" onclick="filterSites('Fair', this)">Fair</button>
        <button class="tab deg-btn" onclick="filterSites('Poor', this)">Poor (<2.5)</button>
    </div>
    
    <div id="site-list"></div>
</div>

<div id="degradation" class="page">
    <div style="background: var(--card); padding: 1rem; border-radius: 12px; margin-bottom: 1rem;">
        <small>Degradation matches PC Dashboard logic (First Month vs Last Month 95th Percentile)</small>
    </div>
    
    <div style="display:flex; gap:0.5rem; overflow-x:auto; margin-bottom:1rem; padding-bottom:5px;">
        <button class="tab deg-btn active" id="btn-offline" onclick="renderDeg('Offline')">Offline 🔴</button>
        <button class="tab deg-btn" id="btn-high" onclick="renderDeg('High')">High (>50%)</button>
        <button class="tab deg-btn" id="btn-medium" onclick="renderDeg('Medium')">Med (30-50%)</button>
        <button class="tab deg-btn" id="btn-low" onclick="renderDeg('Low')">Low (0-30%)</button>
        <button class="tab deg-btn" id="btn-better" onclick="renderDeg('Better')">Better 🟢</button>
    </div>
    <div id="deg-list"></div>
</div>

<div id="perf" class="page">
    <h3 class="cat-header">By Province</h3><div class="grid" id="prov-grid"></div>
    <h3 class="cat-header">By Panel Type</h3><div class="grid" id="panel-grid"></div>
    <h3 class="cat-header">By Project</h3><div class="grid" id="proj-grid"></div>
</div>

<div id="modal" class="modal" onclick="if(event.target==this)closeModal()">
    <div class="modal-content">
        <div style="padding:1rem; background:var(--blue); color:white; display:flex; justify-content:space-between; align-items:center;">
            <div style="overflow:hidden;">
                <h3 id="m-title" style="margin:0; font-size:1.1rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">Site</h3>
                <small id="m-sub" style="opacity:0.8"></small>
            </div>
            <span onclick="closeModal()" style="font-size:2rem; cursor:pointer; padding-left:1rem;">&times;</span>
        </div>
        
        <div style="padding:1rem; overflow-y: auto; max-height: 80vh;">
            <div class="grid">
                <div class="card"><small>Specific Yield (30d)</small><b id="m-y30" style="display:block; font-size:1.4rem"></b></div>
                <div class="card"><small>Array Size</small><b id="m-kwp" style="display:block; font-size:1.4rem"></b></div>
            </div>
            
            <div style="background:rgba(0,0,0,0.03); padding:1rem; border-radius:8px; margin:1rem 0; border: 1px solid rgba(0,0,0,0.1);">
                <div id="m-meta" style="display:grid; grid-template-columns:1fr 1fr; gap:0.8rem; font-size:0.85rem"></div>
            </div>
            
            <h4 style="margin-bottom:0.5rem">Daily Production (kWh)</h4>
            <div style="height:200px; position:relative;"><canvas id="m-chart"></canvas></div>
            
            <h4 style="margin-top:1.5rem; margin-bottom:0.5rem">Specific Yield Trend (kWh/kWp)</h4>
            <div style="height:200px; position:relative;"><canvas id="m-yield-chart"></canvas></div>
        </div>
    </div>
</div>

<script>
    // Embed lightweight metadata only
    const sites = {json_metadata};
    const charts = {json_charts};
    const provs = {json_provs};
    const projs = {json_projs};
    const panels = {json_panels};
    const dist = {json_dist};
    
    const $ = id => document.getElementById(id);
    const siteArr = Object.values(sites);
    let myChart1, myChart2;
    let currentFilter = 'All';
    let currentSearch = '';

    function init() {{
        renderSites('');
        renderDeg('Offline');
        renderCats();
        initCharts();
    }}

    function nav(id, el) {{
        document.querySelectorAll('.page').forEach(el => el.classList.remove('active'));
        $(id).classList.add('active');
        document.querySelectorAll('.tabs .tab').forEach(el => el.classList.remove('active'));
        el.classList.add('active');
    }}

    function toggleTheme() {{
        const b = document.body;
        b.setAttribute('data-theme', b.getAttribute('data-theme')==='dark' ? 'light' : 'dark');
    }}

    function filterSites(cat, el) {{
        currentFilter = cat;
        if(el) {{
            el.parentElement.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
            el.classList.add('active');
        }}
        renderSites(currentSearch);
    }}

    function renderSites(q) {{
        currentSearch = q.toLowerCase();
        let html = '';
        
        let fil = siteArr.filter(s => 
            s.name.toLowerCase().includes(currentSearch) || 
            s.id.toLowerCase().includes(currentSearch) || 
            s.prov.toLowerCase().includes(currentSearch) ||
            s.panel.toLowerCase().includes(currentSearch)
        );

        if(currentFilter !== 'All') {{
            fil = fil.filter(s => s.cat === currentFilter);
        }}

        // Limit to 50 items for DOM performance unless searching
        const displayLimit = currentSearch.length > 0 ? 100 : 50;
        const totalFound = fil.length;
        fil = fil.slice(0, displayLimit);
        
        fil.forEach(s => {{
            const c = s.cat === 'Excellent' ? 'exc' : (s.cat === 'Good' ? 'good' : (s.cat === 'Fair' ? 'fair' : 'poor'));
            html += `<div class="site-item c-${{c}}" onclick="openModal('${{s.id}}')">
                <div class="badg bg-${{c}}">${{s.yld.toFixed(2)}}</div>
                <b>${{s.name}}</b>
                <div style="font-size:0.85rem; opacity:0.7; margin-top:4px">${{s.prov}} • ${{s.kwp}} kWp • ${{s.panel}}</div>
            </div>`;
        }});
        
        if(totalFound > displayLimit) html += `<div style="text-align:center; padding:1rem; color:var(--blue)">+ ${{totalFound - displayLimit}} more sites (use search)</div>`;
        
        $('site-list').innerHTML = fil.length ? html : '<div style="text-align:center; padding:2rem; opacity:0.6">No sites found</div>';
    }}

    function renderDeg(type) {{
        document.querySelectorAll('#degradation .deg-btn').forEach(b => b.classList.remove('active'));
        if(type==='Offline') $('btn-offline').classList.add('active');
        else if(type==='High') $('btn-high').classList.add('active');
        else if(type==='Medium') $('btn-medium').classList.add('active');
        else if(type==='Low') $('btn-low').classList.add('active');
        else $('btn-better').classList.add('active');

        let list = [];
        let html = '';
        
        list = siteArr.filter(s => s.deg_cat === type);
        
        // Sort: High numbers first for Offline/High, low for Better
        if(type === 'Better') list.sort((a,b) => a.deg_act - b.deg_act);
        else list.sort((a,b) => b.deg_act - a.deg_act);

        if(list.length === 0) html += '<div style="text-align:center; padding:2rem; opacity:0.6">No sites in this category</div>';

        list.forEach(s => {{
             const val = type==='Offline' ? 'OFF' : s.deg_act + '%';
             let color = 'fair';
             if(type==='Offline' || type==='High') color = 'poor';
             if(type==='Low' || type==='Better') color = 'good';
             
             let sub = type==='Offline' ? 'Check connectivity (No data 3 days)' : 
                       `Exp: ${{s.deg_exp}}% • Act: ${{s.deg_act}}% (${{s.years}} yrs)`;

             if(type==='Better') sub = `Improving: ${{Math.abs(s.deg_act)}}% better than install`;

             html += `<div class="site-item c-${{color}}" onclick="openModal('${{s.id}}')">
                <div class="badg bg-${{color}}">${{val}}</div>
                <b>${{s.name}}</b>
                <div style="font-size:0.85rem; opacity:0.7; margin-top:4px">${{sub}}</div>
            </div>`;
        }});
        $('deg-list').innerHTML = html;
    }}

    function renderCats() {{
        const gen = (obj) => {{
            let h = '';
            Object.entries(obj).sort((a,b)=>b[1]-a[1]).forEach(([k,v]) => {{
                const c = v > 4.0 ? 'green' : (v > 3.0 ? 'orange' : 'red');
                h += `<div class="card" style="padding:12px; display:flex; justify-content:space-between; align-items:center; border-left:3px solid ${{c}}">
                    <b style="font-size:0.85rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:70%">${{k}}</b>
                    <span style="font-weight:bold; color:${{c}}">${{v.toFixed(2)}}</span>
                </div>`;
            }});
            return h;
        }};
        $('prov-grid').innerHTML = gen(provs);
        $('proj-grid').innerHTML = gen(projs);
        $('panel-grid').innerHTML = gen(panels);
    }}

    // LAZY LOADING: Fetch specific site JSON only when clicked
    async function openModal(id) {{
        const s = sites[id];
        $('modal').classList.add('open');
        $('m-title').innerText = s.name;
        $('m-sub').innerText = s.panel;
        $('m-y30').innerText = s.yld.toFixed(2);
        $('m-kwp').innerText = s.kwp + ' kWp';
        
        $('m-meta').innerHTML = `
            <div>Prov: <b>${{s.prov}}</b></div>
            <div>Grid: <b>${{s.grid}}</b></div>
            <div>Proj: <b>${{s.proj}}</b></div>
            <div>Comm: <b>${{s.comm}}</b></div>
            <div>Src: <b>${{s.src}}</b></div>
            <div>Load: <b>${{s.load || '-'}} kW</b></div>
        `;
        
        // Clear previous charts
        if(myChart1) myChart1.destroy();
        if(myChart2) myChart2.destroy();
        
        // Show loading state in canvas areas
        const ctx1 = $('m-chart').getContext('2d');
        ctx1.clearRect(0,0,300,150);
        ctx1.fillText("Loading data...", 10, 50);

        try {{
            // FETCH INDIVIDUAL FILE
            const res = await fetch(`site_data/${{id}}.json`);
            if(!res.ok) throw new Error("Data not found");
            const data = await res.json();
            
            // Only show last 90 days for better mobile view
            const hist = data.hist.slice(-90);

            myChart1 = new Chart($('m-chart'), {{
                type: 'bar',
                data: {{
                    labels: hist.map(x => x.d.slice(5)), // MM-DD
                    datasets: [{{ label:'Prod (kWh)', data:hist.map(x=>x.v), backgroundColor:'#3498db' }}]
                }},
                options: {{ maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }} }}
            }});

            myChart2 = new Chart($('m-yield-chart'), {{
                type: 'line',
                data: {{
                    labels: hist.map(x => x.d.slice(5)),
                    datasets: [{{ 
                        label:'Yield', 
                        data:hist.map(x=>x.y), 
                        borderColor:'#27ae60', 
                        tension:0.3, 
                        pointRadius:1, 
                        fill: true,
                        backgroundColor: 'rgba(39, 174, 96, 0.1)'
                    }}]
                }},
                options: {{ maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }} }}
            }});
        }} catch(e) {{
            alert("Could not load detailed history for this site.");
            closeModal();
        }}
    }}
    
    function closeModal() {{ $('modal').classList.remove('open'); }}

    function initCharts() {{
        new Chart($('distChart'), {{
            type: 'doughnut',
            data: {{ labels: Object.keys(dist), datasets: [{{ data: Object.values(dist), backgroundColor:['#27ae60','#3498db','#f39c12','#e74c3c'] }}] }},
            options: {{ responsive: true, maintainAspectRatio: false }}
        }});
        new Chart($('gridChart'), {{
            type: 'pie',
            data: {{ labels: Object.keys(charts.grid_access), datasets: [{{ data: Object.values(charts.grid_access), backgroundColor:['#3498db','#9b59b6','#e67e22','#2ecc71'] }}] }},
            options: {{ maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }} }}
        }});
        new Chart($('powerChart'), {{
            type: 'pie',
            data: {{ labels: Object.keys(charts.power_sources), datasets: [{{ data: Object.values(charts.power_sources), backgroundColor:['#e74c3c','#f1c40f','#34495e'] }}] }},
            options: {{ maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }} }}
        }});
        new Chart($('commChart'), {{
            type: 'line',
            data: {{ labels: Object.keys(charts.commissioning), datasets: [{{ label:'Sites', data: Object.values(charts.commissioning), borderColor:'#3498db', pointRadius:0 }}] }},
            options: {{ maintainAspectRatio: false, scales: {{ x: {{ ticks: {{ display: false }} }} }} }}
        }});
    }}

    init();
</script>
</body>
</html>"""

    with open(output_dir / "index.html", "w", encoding='utf-8') as f:
        f.write(html)

    print("✓ Optimized Mobile Dashboard Created!")

if __name__ == "__main__":
    generate_mobile_site()
