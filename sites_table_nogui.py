import pandas as pd
import subprocess
import sys  
from pathlib import Path
from datetime import datetime
import numpy as np
import shutil

# Add this block after "import shutil" and before "def load_historical_data"

def check_and_install_requirements():
    """Check if required packages are installed, auto-install if missing"""
    required = {
        'pandas': 'pandas',
        'openpyxl': 'openpyxl', 
        'pyarrow': 'pyarrow',
        'numpy': 'numpy'
    }
    missing = []
    
    # Check which packages are missing
    for package, import_name in required.items():
        try:
            __import__(import_name)
        except ImportError:
            missing.append(package)
    
    # If packages are missing, install them
    if missing:
        print("\n" + "="*70)
        print("INSTALLING MISSING PACKAGES")
        print("="*70)
        print(f"\nThe following packages will be installed:")
        for pkg in missing:
            print(f"  - {pkg}")
        print("\nInstalling... Please wait...\n")
        
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
            print("\n✓ All packages installed successfully!")
            print("="*70 + "\n")
            return True
        except subprocess.CalledProcessError as e:
            print(f"\n✗ Installation failed: {e}")
            print("\nPlease try installing manually:")
            print(f"  pip install {' '.join(missing)}")
            print("="*70 + "\n")
            return False
    
    return True

def load_historical_data(history_file):
    """Load historical data from parquet file"""
    if history_file.exists():
        print(f"  ✓ Loading historical data from: {history_file.name}")
        try:
            df = pd.read_parquet(history_file)
            print(f"  ✓ Loaded {len(df):,} historical records")
            return df
        except Exception as e:
            print(f"  ⚠ Error loading history file: {e}")
            return None
    else:
        print(f"  ℹ No historical data found. Will create new history file.")
        return None

def save_historical_data(df, history_file):
    """Save combined data to parquet file for future use"""
    try:
        df.to_parquet(history_file, index=False, compression='snappy')
        print(f"  ✓ Historical data saved to: {history_file.name}")
        return True
    except Exception as e:
        print(f"  ⚠ Error saving history file: {e}")
        return False

def move_files_to_archive(xlsx_files, archive_folder):
    """Move processed Excel files to archive folder"""
    print("\n  Moving processed files to archive...")
    
    # Create archive folder if it doesn't exist
    archive_folder = Path(archive_folder)
    archive_folder.mkdir(exist_ok=True)
    
    moved_count = 0
    failed_count = 0
    
    for file in xlsx_files:
        try:
            # Create destination path
            dest_path = archive_folder / file.name
            
            # If file exists in archive, add timestamp to avoid overwrite
            if dest_path.exists():
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                stem = dest_path.stem
                suffix = dest_path.suffix
                dest_path = archive_folder / f"{stem}_{timestamp}{suffix}"
            
            # Move file
            shutil.move(str(file), str(dest_path))
            print(f"    ✓ Moved: {file.name}")
            moved_count += 1
            
        except Exception as e:
            print(f"    ✗ Failed to move {file.name}: {e}")
            failed_count += 1
    
    print(f"\n  ✓ Archive complete: {moved_count} moved, {failed_count} failed")
    return moved_count, failed_count

def load_monitoring_data(monitoring_folder, historical_df=None, archive_folder=None):
    """Load monitoring data from Excel files and merge with historical data"""
    print("\n[2/5] Loading monitoring data from Excel files...")
    
    folder = Path(monitoring_folder)
    xlsx_files = [f for f in folder.glob("*.xlsx") if not f.name.startswith("~$")]
    
    if not xlsx_files:
        print("  ⚠ No new Excel files found in monitoring folder")
        if historical_df is not None:
            return historical_df, []
        return None, []
    
    print(f"  Found {len(xlsx_files)} Excel files")
    
    all_data = []
    successfully_read_files = []
    
    for file in xlsx_files:
        try:
            print(f"    Reading: {file.name}...")
        
            # 1. Scan first 30 rows to find the specific column header
            df_scan = pd.read_excel(file, nrows=30, header=None)
            
            header_row_idx = None
            for i, row in df_scan.iterrows():
                # Convert row to string for searching
                row_str = " ".join([str(val) for val in row.values])
                
                # We look for the specific unique column name from your screenshot
                if "Solar Supply (kWh)" in row_str:
                    header_row_idx = i
                    print(f"      ✓ Found headers at Row {i+1} (Index {i})")
                    break
            
            # If not found, force try Row 21 (Index 20)
            if header_row_idx is None:
                print("      ⚠ Auto-detect failed. Forcing Row 21...")
                header_row_idx = 20
            
            # 2. Load the data using the found index
            df = pd.read_excel(file, header=header_row_idx)
            
            # Clean column names (strip whitespace like "Site " -> "Site")
            df.columns = df.columns.astype(str).str.strip()
            
            # 3. Flexible Column Matching
            # We look for columns that *contain* the key words, case-insensitive
            col_site = None
            col_date = None
            col_solar = None
            
            for col in df.columns:
                c_low = col.lower()
                # strict match for site to avoid 'Site ID' unless 'Site' is missing
                if c_low == 'site': 
                    col_site = col
                elif c_low == 'date':
                    col_date = col
                elif 'solar' in c_low and 'supply' in c_low:
                    col_solar = col
            
            # Print what we found
            print(f"      Columns detected: Site='{col_site}', Date='{col_date}', Solar='{col_solar}'")
            
            if col_site and col_date and col_solar:
                # Rename and process
                df_subset = df[[col_site, col_date, col_solar]].copy()
                df_subset.columns = ['Site_ID', 'Date', 'Solar_kWh']
                
                # Standardize data
                df_subset['Site_ID'] = df_subset['Site_ID'].astype(str).str.strip()
                df_subset['Date'] = pd.to_datetime(df_subset['Date'], errors='coerce')
                df_subset['Solar_kWh'] = pd.to_numeric(df_subset['Solar_kWh'], errors='coerce')
                
                # Drop invalid rows
                df_subset = df_subset.dropna(subset=['Date'])
                
                if len(df_subset) > 0:
                    all_data.append(df_subset)
                    successfully_read_files.append(file)
                    print(f"      ✓ Loaded {len(df_subset)} valid records")
                else:
                    print("      ⚠ File loaded but contained 0 valid data rows")
            else:
                print("      ✗ Missing specific columns. Expected: Site, Date, Solar Supply (kWh)")
                print(f"      Available columns: {list(df.columns)}")

        except Exception as e:
            print(f"      ✗ Error processing file: {e}")

    # Process results as before
    if not all_data:
        print("\n  ⚠ No valid data extracted")
        return (historical_df, []) if historical_df is not None else (None, [])
        
    # Combine
    new_data_df = pd.concat(all_data, ignore_index=True)
    
    if historical_df is not None:
        combined_df = pd.concat([historical_df, new_data_df], ignore_index=True)
    else:
        combined_df = new_data_df
        
    # Deduplicate
    combined_df = combined_df.sort_values('Date').drop_duplicates(subset=['Site_ID', 'Date'], keep='last')
    
    # Archive
    if archive_folder and successfully_read_files:
        move_files_to_archive(successfully_read_files, archive_folder)
        
    return combined_df, successfully_read_files

def build_installed_sites_table(monitoring_folder, metadata_file, output_file=None, history_file=None, archive_folder=None):
    """
    Build a comprehensive table with:
    - Site metadata from solar_installation_info.xlsx
    - Daily solar production for each date (merged with historical data)
    - Summary statistics for 7, 30, 90 days and all time
    """
    
    # Setup file paths
    base_path = Path(monitoring_folder).parent

    if history_file is None:
        history_file = base_path / "monitoring_data_history.parquet"
    else:
        history_file = Path(history_file)  # ADD THIS LINE - Convert string to Path
    
    if output_file is None:
        timestamp = datetime.now().strftime('%d%m%Y')
        output_file = base_path / f"installed_sites_production_{timestamp}.xlsx"
    else:
        output_file = Path(output_file)  # ADD THIS LINE

    if archive_folder is None:
        archive_folder = base_path / "Archives"
    else:
        archive_folder = Path(archive_folder)  # ADD THIS LINE
    
    print("="*70)
    print("INSTALLED SITES PRODUCTION TABLE BUILDER")
    print("="*70)
    print(f"\nHistory file: {history_file}")
    print(f"Archive folder: {archive_folder}")
    print(f"Output file: {output_file}")
    
    # Step 1: Load site metadata
    print("\n[1/5] Loading installed sites metadata...")
    try:
        metadata_df = pd.read_excel(metadata_file)
        print(f"  ✓ Loaded {len(metadata_df)} sites from metadata file")
        
        # Clean the Split column to use as Site_ID
        metadata_df['Site_ID'] = metadata_df['Split'].str.strip()
        
        # Calculate Array Size from Panels and Panel Size (handle NA values)
        def calculate_array_size(row):
            try:
                panels = pd.to_numeric(row['Panels'], errors='coerce')
                panel_size = pd.to_numeric(row['Panel Size'], errors='coerce')
                if pd.notna(panels) and pd.notna(panel_size) and panels > 0 and panel_size > 0:
                    return (panels * panel_size) / 1000
                return 0
            except:
                return 0
        
        metadata_df['Array_Size_kWp'] = metadata_df.apply(calculate_array_size, axis=1)
        
        # Create Panel Description (combination of size, vendor, and model)
        def create_panel_description(row):
            try:
                panel_size = pd.to_numeric(row['Panel Size'], errors='coerce')
                if pd.notna(panel_size) and panel_size > 0:
                    size_str = str(int(panel_size))
                else:
                    size_str = "Unknown"
                
                vendor = str(row['Panel Vendor']) if pd.notna(row['Panel Vendor']) else "Unknown"
                model = str(row['Panel Model']) if pd.notna(row['Panel Model']) else "Unknown"
                
                return f"{size_str} {vendor} {model}"
            except:
                return "Unknown Panel"
        
        metadata_df['Panel_Description'] = metadata_df.apply(create_panel_description, axis=1)
        
        # Keep essential metadata columns
        metadata_cols = ['Site_ID', 'Site', 'Split', 'PO', 'Project', 'Grid Access', 
                        'Power Sources', 'Panels', 'Panel Size', 'Panel Model', 
                        'Panel Vendor', 'Panel_Description', 'Array_Size_kWp', 'Avg Load']
        
        # Only include columns that exist
        metadata_cols = [col for col in metadata_cols if col in metadata_df.columns]
        metadata_df = metadata_df[metadata_cols].copy()
        
    except Exception as e:
        print(f"  ✗ Error loading metadata: {e}")
        return False
    
    # Step 2: Load historical data
    historical_df = load_historical_data(history_file)
    
    # Step 3: Load and merge monitoring data (files will be moved to archive after reading)
    combined_df, processed_files = load_monitoring_data(monitoring_folder, historical_df, archive_folder)
    
    if combined_df is None or len(combined_df) == 0:
        print("\n  ✗ No valid data available")
        return False
    
    # Save updated historical data
    print("\n  Updating historical data file...")
    save_historical_data(combined_df, history_file)
    
    # Combine all data and pivot
    print("\n[3/5] Pivoting data...")
    print(f"  ✓ Total records: {len(combined_df):,}")
    
    # Get unique dates
    all_dates = sorted(combined_df['Date'].unique())
    all_dates = pd.date_range(start=min(all_dates), end=max(all_dates), freq='D')
    print(f"  ✓ Date range: {pd.to_datetime(all_dates[0]).strftime('%Y-%m-%d')} to {pd.to_datetime(all_dates[-1]).strftime('%Y-%m-%d')}")
    print(f"  ✓ Total unique dates: {len(all_dates)}")
    
    # Pivot the data
    print("\n  Pivoting data (this may take a moment)...")
    pivot_df = combined_df.pivot(index='Site_ID', columns='Date', values='Solar_kWh')
    pivot_df = pivot_df.reset_index()
    
    print(f"  ✓ Matrix created: {len(pivot_df)} sites × {len(pivot_df.columns)-1} dates")
    
    # Step 4: Merge with metadata
    print("\n[4/5] Merging with site metadata...")
    final_df = metadata_df.merge(pivot_df, on='Site_ID', how='left')
    print(f"  ✓ Final matrix: {len(final_df)} sites")
    
    # Format date columns
    date_columns = [col for col in final_df.columns if isinstance(col, pd.Timestamp)]
    column_rename = {col: col.strftime('%Y-%m-%d') for col in date_columns}
    final_df = final_df.rename(columns=column_rename)
    
    # Calculate summary statistics
    print("\n  Calculating summary statistics...")
    date_col_names = [col for col in final_df.columns if col not in metadata_cols]
    
    if date_col_names:
        # Get current date for time-based calculations
        latest_date = pd.to_datetime(date_col_names[-1])
        
        # Calculate for different time periods
        date_7d = (latest_date - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
        date_30d = (latest_date - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
        date_90d = (latest_date - pd.Timedelta(days=90)).strftime('%Y-%m-%d')
        
        # Get columns for each period
        cols_7d = [col for col in date_col_names if col >= date_7d]
        cols_30d = [col for col in date_col_names if col >= date_30d]
        cols_90d = [col for col in date_col_names if col >= date_90d]
        
        # 7-day statistics
        final_df['Prod_7d_kWh'] = final_df[cols_7d].sum(axis=1)
        final_df['Avg_Daily_7d_kWh'] = final_df[cols_7d].mean(axis=1, skipna=True)
        final_df['Avg_Yield_7d_kWh_kWp'] = final_df.apply(
            lambda row: row['Avg_Daily_7d_kWh'] / row['Array_Size_kWp']
            if pd.notna(row['Array_Size_kWp']) and row['Array_Size_kWp'] > 0
            else None,
            axis=1
        )
        
        # 30-day statistics
        final_df['Prod_30d_kWh'] = final_df[cols_30d].sum(axis=1)
        final_df['Avg_Daily_30d_kWh'] = final_df[cols_30d].mean(axis=1, skipna=True)
        final_df['Avg_Yield_30d_kWh_kWp'] = final_df.apply(
            lambda row: row['Avg_Daily_30d_kWh'] / row['Array_Size_kWp']
            if pd.notna(row['Array_Size_kWp']) and row['Array_Size_kWp'] > 0
            else None,
            axis=1
        )
        
        # 90-day statistics
        final_df['Prod_90d_kWh'] = final_df[cols_90d].sum(axis=1)
        final_df['Avg_Daily_90d_kWh'] = final_df[cols_90d].mean(axis=1, skipna=True)
        final_df['Avg_Yield_90d_kWh_kWp'] = final_df.apply(
            lambda row: row['Avg_Daily_90d_kWh'] / row['Array_Size_kWp']
            if pd.notna(row['Array_Size_kWp']) and row['Array_Size_kWp'] > 0
            else None,
            axis=1
        )
        
        # All-time statistics
        final_df['Total_Production_kWh'] = final_df[date_col_names].sum(axis=1)
        final_df['Days_With_Data'] = final_df[date_col_names].notna().sum(axis=1)
        final_df['Avg_Daily_Production_kWh'] = final_df[date_col_names].mean(axis=1, skipna=True)
        final_df['Avg_Specific_Yield_kWh_kWp_day'] = final_df.apply(
            lambda row: row['Avg_Daily_Production_kWh'] / row['Array_Size_kWp']
            if pd.notna(row['Array_Size_kWp']) and row['Array_Size_kWp'] > 0
            else None,
            axis=1
        )
        
        # First production date
        def get_first_production_date(row):
            for col in date_col_names:
                val = row[col]
                if pd.notna(val) and val > 0:
                    return col
            return None
        
        final_df['First_Production_Date'] = final_df.apply(get_first_production_date, axis=1)
    
    # Reorder columns
    summary_cols = ['Prod_7d_kWh', 'Avg_Daily_7d_kWh', 'Avg_Yield_7d_kWh_kWp',
                    'Prod_30d_kWh', 'Avg_Daily_30d_kWh', 'Avg_Yield_30d_kWh_kWp',
                    'Prod_90d_kWh', 'Avg_Daily_90d_kWh', 'Avg_Yield_90d_kWh_kWp',
                    'Total_Production_kWh', 'Days_With_Data', 'Avg_Daily_Production_kWh',
                    'First_Production_Date', 'Avg_Specific_Yield_kWh_kWp_day']
    
    existing_summary_cols = [col for col in summary_cols if col in final_df.columns]
    column_order = metadata_cols + existing_summary_cols + date_col_names
    final_df = final_df[column_order]
    
    # Step 5: Save to Excel
    print(f"\n[5/5] Saving to Excel...")
    print(f"  Output file: {output_file}")
    
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            final_df.to_excel(writer, sheet_name='Installed Sites Production', index=False)
            
            worksheet = writer.sheets['Installed Sites Production']
            
            # Auto-adjust column widths
            for idx, col in enumerate(metadata_cols + existing_summary_cols, 1):
                max_length = max(
                    final_df[col].astype(str).apply(len).max(),
                    len(col)
                ) + 2
                worksheet.column_dimensions[worksheet.cell(1, idx).column_letter].width = min(max_length, 25)
            
            # Freeze panes
            worksheet.freeze_panes = 'N2'
        
        print(f"  ✓ File saved successfully!")
        
        # Print summary
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        print(f"Total Sites: {len(final_df)}")
        print(f"Date Columns: {len(date_col_names)}")
        print(f"Date Range: {date_col_names[0] if date_col_names else 'N/A'} to {date_col_names[-1] if date_col_names else 'N/A'}")
        
        sites_with_solar = (final_df['Days_With_Data'] > 0).sum()
        print(f"Sites with solar data: {sites_with_solar}")
        
        # Top 5 producers
        if 'Total_Production_kWh' in final_df.columns:
            print("\nTop 5 Producers (Total kWh):")
            top5 = final_df.nlargest(5, 'Total_Production_kWh')[['Site_ID', 'Site', 'Total_Production_kWh', 'Days_With_Data']]
            for idx, row in top5.iterrows():
                print(f"  {row['Site_ID']}: {row['Total_Production_kWh']:.1f} kWh ({int(row['Days_With_Data'])} days)")
        
        print("\n" + "="*70)
        print("COMPLETE! Open the Excel file to view all production data.")
        print("="*70)
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error saving file: {e}")
        return False


def main():
    """Main entry point"""
    # Use current directory where the script is running
    BASE_PATH = Path(__file__).parent.resolve()
        # ADD THESE 3 LINES HERE:
    if not check_and_install_requirements():
        print("\n❌ Cannot proceed without required packages. Exiting...")
        return
    # Set your file paths
    monitoring_folder = BASE_PATH / "monitoring_data"  # Folder containing NEW monitoring Excel files
    metadata_file = BASE_PATH / "solar_installation_info.xlsx"  # Your installed sites metadata file
    history_file = BASE_PATH / "monitoring_data_history.parquet"  # Historical data (auto-managed)
    archive_folder = BASE_PATH / "Archives"  # Archive folder for processed files
    
    # Output file with timestamp
    timestamp = datetime.now().strftime('%d%m%Y')
    output_file = BASE_PATH / f"installed_sites_production_{timestamp}.xlsx"
    
    print("="*70)
    print("INSTALLED SITES PRODUCTION TABLE BUILDER")
    print("="*70)
    print(f"\nMonitoring folder: {monitoring_folder}")
    print(f"Metadata file: {metadata_file}")
    print(f"History file: {history_file}")
    print(f"Archive folder: {archive_folder}")
    print(f"Output file: {output_file}")
    print("\nStarting process...\n")
    
    # Check if paths exist
    if not monitoring_folder.exists():
        print(f"ERROR: Monitoring folder not found: {monitoring_folder}")
        return
    
    if not metadata_file.exists():
        print(f"ERROR: Metadata file not found: {metadata_file}")
        return
    
    # Run the builder
    success = build_installed_sites_table(
        str(monitoring_folder),
        str(metadata_file),
        str(output_file),
        str(history_file),
        str(archive_folder)
    )
    
    if success:
        print(f"\n✓ SUCCESS! Output file created at:\n  {output_file}")
        print(f"\nℹ Historical data saved at:\n  {history_file}")
        print(f"\nℹ Processed files archived at:\n  {archive_folder}")
        print("\n✓ Monitoring folder is now ready for new files!")
    else:
        print("\n✗ FAILED! Check the error messages above.")


if __name__ == "__main__":
    main()
