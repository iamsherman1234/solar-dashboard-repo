# Solar Performance Dashboard & Automation System

**Version:** 2.0 (Cloud-Native Edition)  
**Status:** ✅ Automated (Daily at 07:00 AM ICT)

A fully automated, cloud-native system for monitoring, analyzing, and visualizing solar performance across **1,770 installations**.  
The automation is powered by **GitHub Actions** and integrated with **Google Drive** for storage and file synchronization.

## 📋 Table of Contents
- Overview & Architecture
- Features
- Cloud Configuration (Google Drive)
- Project Structure
- Operational Workflow
- Input Data Specifications
- Output Artifacts
- Setup Guide (Deployment)
- Troubleshooting
- Technical Details

## 🎯 Overview & Architecture

This system replaces manual Excel processing with a fully automated CI/CD workflow. It pulls monitoring files from Google Drive, processes them in GitHub Actions, and publishes dashboards and reports back to Drive.

### 🔗 Data Pipeline
1. **Ingest:** Download new `.xlsx` monitoring files from Google Drive (`01_Monitoring_Data`).
2. **Restore:** Load historical cache (`monitoring_data_history.parquet`) for fast processing of **2,383+ days**.
3. **Process:** Clean, consolidate, and compute yield and degradation metrics.
4. **Visualize:** Generate a serverless, interactive dashboard (HTML).
5. **Publish:** Upload final reports and archive processed files.

## ✨ Features

### ☁️ Cloud Automation
- Automated daily run at **07:00 AM Phnom Penh Time**.
- Full read/write Google Drive integration.
- Self-healing: auto-installs dependencies and restores cache.
- No servers, laptops, or manual execution required.

### 📊 Analytics & Visualization
- **Specific Yield:** 7-day, 30-day, 90-day, all-time.
- **Degradation Detection:** Offline site detection & lifecycle degradation checks.
- **Dashboard Features:**  
  - Interactive charts  
  - Search & filters  
  - Dark mode  
  - Site drill-down  
- **Grouping:** Province, Project, Panel Type, Vendor, etc.

## ☁️ Cloud Configuration (Google Drive)

The system uses **three controlled folders**.  
⚠️ **Do not rename or move these without updating `drive_manager.py`.**

| Folder Name | Folder ID | Purpose |
|------------|-----------|---------|
| **01_Monitoring_Data** | `1ZCVjpjqZ5rnLBhCTZf2yeQbEOX9zeYCm` | Raw input files |
| **02_Archives** | `19AJmzhnlwXI78B0HTNX3mke8sMr-XK1G` | Processed file archive |
| **Solar_Project_Master** | `1jhw0lRHwG8ogRCL9g9Qu3RAsN0gkNLPl` | Final dashboard & reports |

## 📁 Project Structure

```
solar-dashboard-repo/
├── .github/
│   └── workflows/
│       └── daily_monitor.yml        # Automation workflow (runs daily)
│
├── drive_manager.py                 # Google Drive download/upload/sync operations
├── sites_table_nogui.py             # Data processing engine
├── dashboard_generator.py           # HTML dashboard builder
├── solar_installation_info.xlsx     # Site metadata
├── solar_performance.db             # SQLite: panel & system details
├── requirements.txt                 # Python dependencies
└── README.md                        # Documentation
```

## 🔄 Operational Workflow

### For Data Managers
1. Upload raw `.xlsx` files to **01_Monitoring_Data**.
2. The system processes everything at **07:00 AM** daily.
3. View results in **Solar_Project_Master**:
   - `installed_sites_dashboard_[DATE].html`
   - `installed_sites_production_[DATE].xlsx`

### For Developers (Manual Trigger)
1. Go to the GitHub repo → **Actions**
2. Open **Solar Dashboard Automation**
3. Click **Run workflow**
4. Wait 5–10 minutes for completion

## 📊 Input Data Specifications

### 1. Site Metadata (`solar_installation_info.xlsx`)
Required columns:
- `Split` (**must match** monitoring file Site ID)
- `Panels`
- `Panel Size`
- `Project`
- `Province`

### 2. Monitoring Data (Excel)
- Format: `.xlsx`
- Required columns:
  - `Site`
  - `Date`
  - `Solar Supply (kWh)`
- Header row auto-detection supported.

## 📤 Output Artifacts

| Output File | Description | Location |
|-------------|-------------|----------|
| **Dashboard HTML** | Interactive visualization | Solar_Project_Master + GitHub Releases |
| **Production Report (Excel)** | Full daily dataset for all sites | Solar_Project_Master + GitHub Releases |
| **Historical Cache (Parquet)** | Consolidated long-term dataset | Solar_Project_Master |

## ⚙️ Setup Guide (Deployment)

### 1. Google Cloud Platform (GCP)
- Create a **Service Account**
- Enable **Google Drive API**
- Download the **JSON Key**

### 2. Google Drive Permissions
Share all three Drive folders with the Service Account email:  
**Editor access required**

### 3. GitHub Actions Secret
Add repository secret:  
- `GDRIVE_CREDENTIALS` → paste JSON key content

### 4. Upload Code
Ensure all Python scripts and `requirements.txt` are in the main branch.

## 🔧 Troubleshooting

| Error | Cause | Fix |
|------|--------|------|
| **HttpError 403 / 404** | Missing Drive permissions | Share folders with Service Account |
| **ModuleNotFoundError** | Missing dependency | Add to `requirements.txt` |
| **Site ID mismatch** | Metadata inconsistency | Ensure `Split` = `Site` column exactly |
| **Workflow timeout** | Cache missing | Ensure `monitoring_data_history.parquet` loads properly |

## 📘 Technical Details

Uses:
- pandas  
- SQLite  
- Plotly/HTML templating  
- Google Drive API  
- GitHub Actions CI/CD  

