# Solar Performance Dashboard & Automation System

**Version:** 2.0 (Cloud-Native Edition)  
**Status:** ✅ Automated (Daily 07:00 AM ICT)

A comprehensive, cloud-automated system for monitoring, analyzing, and visualizing solar site performance across **1,770 installations**. This system leverages **GitHub Actions** for compute and **Google Drive** for storage, automating the entire data pipeline from ingestion to dashboard publication.

## 📋 Table of Contents

- [Overview & Architecture](#-overview--architecture)
- [Features](#-features)
- [Cloud Configuration](#-cloud-configuration-google-drive)
- [Project Structure](#-project-structure)
- [Operational Workflow](#-operational-workflow)
- [Input Data Specs](#-input-data-specifications)
- [Output Artifacts](#-output-artifacts)
- [Setup Guide](#-setup-guide)
- [Troubleshooting](#-troubleshooting)
- [Technical Details](#-technical-details)

## 🎯 Overview & Architecture

The system replaces manual local processing with a robust CI/CD pipeline. It automatically ingests raw monitoring data from Google Drive, processes it using Python on GitHub's cloud runners, and uploads the results back to Google Drive and GitHub Releases.

### The Pipeline
1.  **Ingest:** Pulls new `.xlsx` monitoring files from **Google Drive (01_Monitoring_Data)**.
2.  **Restore:** Retrieves the historical data cache (`parquet`) to ensure fast processing of **2,383 days** of history.
3.  **Process:** Consolidates data, removes duplicates, and calculates yields/degradation metrics.
4.  **Visualize:** Generates an interactive HTML dashboard.
5.  **Archive & Publish:** Moves processed files to **02_Archives** and saves reports to **Solar_Project_Master**.

## ✨ Features

### ☁️ Cloud Automation
* **Daily Schedule:** Runs automatically at **07:00 AM Phnom Penh Time** (00:00 UTC).
* **Drive Integration:** Seamless 2-way sync with Google Drive APIs.
* **Self-Healing:** Auto-installs dependencies and restores historical cache to prevent timeouts.
* **Zero-Maintenance:** No local server or laptop required for daily operations.

### 📊 Analytics & Visualization
* **Performance Metrics:** 7-day, 30-day, 90-day, and All-time specific yields (kWh/kWp/day).
* **Degradation Analysis:** Automated detection of offline sites and degradation rates vs. expected lifecycle.
* **Interactive Dashboard:** Serverless HTML dashboard with Dark Mode, Search, and Drill-down capabilities.
* **Fleet categorization:** Grouping by Province, Project, Panel Type, and Vendor.

## ☁️ Cloud Configuration (Google Drive)

The system relies on three specific Google Drive folders. Do not rename or delete these folders without updating `drive_manager.py`.

| Folder Name | Drive Folder ID | Purpose |
| :--- | :--- | :--- |
| **01_Monitoring_Data** | `1ZCVjpjqZ5rnLBhCTZf2yeQbEOX9zeYCm` | **Input:** Drop raw Excel/ZIP files here. |
| **02_Archives** | `19AJmzhnlwXI78B0HTNX3mke8sMr-XK1G` | **Storage:** Processed files are moved here automatically. |
| **Solar_Project_Master** | `1jhw0lRHwG8ogRCL9g9Qu3RAsN0gkNLPl` | **Output:** Dashboard HTML & Production Excel appear here. |

## 📁 Project Structure

```
solar-dashboard-repo/
├── .github/
│   └── workflows/
│       └── daily_monitor.yml      # CI/CD: The automation trigger (7AM Daily)
│
├── drive_manager.py               # Core: Handles Google Drive Download/Upload/Sync
├── sites_table_nogui.py           # Core: Data processing & consolidation engine
├── dashboard_generator.py         # Core: HTML visualization generator
├── solar_installation_info.xlsx   # Config: Site Metadata (Split, Capacity, etc.)
├── solar_performance.db           # Config: SQLite database for site details
├── requirements.txt               # Config: Python dependencies
└── README.md                      # Documentation
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

**Maintained by:** [LayAn](https://github.com/iamsherman1234)  
**License:** Proprietary Software

