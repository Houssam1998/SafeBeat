"""
SafeBeat ETL - Clean 911 Calls Data
Cleans and standardizes 911 calls data for analysis

Output: datasets/cleaned/911_calls_cleaned.parquet
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
from datetime import datetime

def clean_911_calls():
    print("=" * 60)
    print("CLEANING 911 CALLS DATA")
    print("=" * 60)
    
    # 1. Load 911 calls data
    print("\n[1/6] Loading 911 calls Excel file...")
    print("   (This may take a few minutes for large files...)")
    
    df = pd.read_excel(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\APD_911_Calls_for_Service_2023-2025_20251223.xlsx'
    )
    print(f"   Total calls loaded: {len(df):,}")
    print(f"   Columns: {df.columns.tolist()}")
    
    # 2. Clean and standardize column names
    print("\n[2/6] Standardizing column names...")
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')
    print(f"   Renamed {len(original_cols)} columns")
    
    # 3. Convert data types
    print("\n[3/6] Converting data types...")
    
    # Convert Geo ID to string with proper formatting (12 digits)
    df['geo_id'] = df['geo_id'].astype(str).str.zfill(12)
    print(f"   ✓ geo_id converted to string (sample: {df['geo_id'].iloc[0]})")
    
    # Datetime columns should already be datetime64, verify
    datetime_cols = ['response_datetime', 'first_unit_arrived_datetime', 'call_closed_datetime']
    for col in datetime_cols:
        if col in df.columns:
            if df[col].dtype != 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            print(f"   ✓ {col}: {df[col].dtype}")
    
    # 4. Handle missing values
    print("\n[4/6] Handling missing values...")
    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    print(f"   Columns with nulls: {len(cols_with_nulls)}")
    for col, count in cols_with_nulls.head(10).items():
        print(f"      {col}: {count:,} ({count/len(df)*100:.1f}%)")
    
    # Fill categorical nulls with 'Unknown'
    categorical_cols = ['incident_type', 'mental_health_flag', 'sector', 
                       'initial_problem_description', 'initial_problem_category',
                       'final_problem_description', 'final_problem_category',
                       'call_disposition_description']
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown')
    
    # 5. Add derived columns
    print("\n[5/6] Adding derived columns...")
    
    # Extract date components from response_datetime
    if 'response_datetime' in df.columns:
        df['response_date'] = df['response_datetime'].dt.date
        df['response_month'] = df['response_datetime'].dt.to_period('M').astype(str)
        df['response_year'] = df['response_datetime'].dt.year
        print("   ✓ Added: response_date, response_month, response_year")
    
    # Create priority level numeric
    priority_map = {
        'Priority 0': 0, 
        'Priority 1': 1, 
        'Priority 2': 2, 
        'Priority 3': 3
    }
    df['priority_numeric'] = df['priority_level'].map(priority_map).fillna(9)
    print("   ✓ Added: priority_numeric")
    
    # 6. Select final columns
    print("\n[6/6] Selecting final columns...")
    final_columns = [
        'incident_number', 'incident_type', 'priority_level', 'priority_numeric',
        'response_datetime', 'response_date', 'response_month', 'response_year',
        'response_day_of_week', 'response_hour', 'response_time',
        'first_unit_arrived_datetime', 'call_closed_datetime',
        'sector', 'geo_id', 'census_block_group', 'council_district',
        'initial_problem_description', 'initial_problem_category',
        'final_problem_description', 'final_problem_category',
        'number_of_units_arrived', 'unit_time_on_scene',
        'mental_health_flag', 'call_disposition_description', 'report_written_flag',
        'officer_injured_killed_count', 'subject_injured_killed_count', 'other_injured_killed_count'
    ]
    
    # Keep only columns that exist
    final_columns = [c for c in final_columns if c in df.columns]
    df_clean = df[final_columns].copy()
    
    # =========================================
    # VERIFICATION CHECKS
    # =========================================
    print("\n" + "=" * 60)
    print("VERIFICATION CHECKS")
    print("=" * 60)
    
    # Check 1: Table not empty
    assert len(df_clean) > 0, "ERROR: Table is empty!"
    print(f"✓ Table has {len(df_clean):,} rows")
    
    # Check 2: geo_id format (12 characters)
    geo_id_lengths = df_clean['geo_id'].str.len().unique()
    print(f"✓ Geo ID lengths: {geo_id_lengths.tolist()}")
    
    # Check 3: Date range
    date_min = df_clean['response_datetime'].min()
    date_max = df_clean['response_datetime'].max()
    print(f"✓ Date range: {date_min} to {date_max}")
    
    # Check 4: Priority distribution
    print("\n   Priority distribution:")
    priority_dist = df_clean['priority_level'].value_counts()
    for priority, count in priority_dist.items():
        print(f"      {priority}: {count:,}")
    
    # Check 5: Incident type distribution (top 10)
    print("\n   Top 10 incident types:")
    type_dist = df_clean['incident_type'].value_counts().head(10)
    for itype, count in type_dist.items():
        print(f"      {itype}: {count:,}")
    
    # =========================================
    # SAMPLE OUTPUT
    # =========================================
    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT (First 5 rows)")
    print("=" * 60)
    sample_cols = ['incident_number', 'priority_level', 'response_datetime', 'geo_id', 'sector']
    print(df_clean[sample_cols].head(5).to_string(index=False))
    
    # =========================================
    # SAVE
    # =========================================
    output_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\911_calls_cleaned.parquet'
    df_clean.to_parquet(output_path, index=False)
    print(f"\n✓ Saved to: {output_path}")
    
    return df_clean

if __name__ == "__main__":
    df = clean_911_calls()
    print("\n" + "=" * 60)
    print("911 CALLS CLEANING COMPLETE!")
    print("=" * 60)
