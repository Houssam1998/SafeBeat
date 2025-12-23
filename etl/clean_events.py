"""
SafeBeat ETL - Clean Events Data
Cleans and standardizes festival/events data for analysis

Output: datasets/cleaned/events_cleaned.parquet
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
from datetime import datetime

def clean_events():
    print("=" * 60)
    print("CLEANING EVENTS DATA")
    print("=" * 60)
    
    # 1. Load events data
    print("\n[1/6] Loading Events Excel file...")
    df = pd.read_excel(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\ACE_Events_20251223.xlsx'
    )
    print(f"   Total events loaded: {len(df):,}")
    print(f"   Columns: {df.columns.tolist()}")
    
    # 2. Standardize column names
    print("\n[2/6] Standardizing column names...")
    df.columns = df.columns.str.strip().str.lower()
    
    # 3. Parse dates
    print("\n[3/6] Parsing date columns...")
    
    # Parse START_DATE and END_DATE (format: "MMM dd, yyyy" like "Aug 02, 2019")
    def parse_date(date_str):
        if pd.isna(date_str) or date_str == '':
            return pd.NaT
        try:
            # Try format like "Aug 02, 2019"
            return pd.to_datetime(date_str, format='%b %d, %Y')
        except:
            try:
                # Fallback to pandas auto-parsing
                return pd.to_datetime(date_str)
            except:
                return pd.NaT
    
    df['start_date'] = df['start_date'].apply(parse_date)
    df['end_date'] = df['end_date'].apply(parse_date)
    
    print(f"   ✓ start_date parsed: {df['start_date'].notna().sum():,} valid dates")
    print(f"   ✓ end_date parsed: {df['end_date'].notna().sum():,} valid dates")
    
    # Date range
    valid_starts = df[df['start_date'].notna()]['start_date']
    if len(valid_starts) > 0:
        print(f"   Date range: {valid_starts.min()} to {valid_starts.max()}")
    
    # 4. Convert flags to boolean
    print("\n[4/6] Converting flags to boolean...")
    
    def to_boolean(val):
        if pd.isna(val):
            return False
        val_str = str(val).strip().lower()
        return val_str in ['yes', 'y', 'true', '1']
    
    df['has_alcohol'] = df['alcohol_served'].apply(to_boolean)
    df['has_amplified_sound'] = df['amplified_sound'].apply(to_boolean)
    df['has_road_closure'] = df['road_closure'].apply(lambda x: pd.notna(x) and str(x).strip() != '')
    
    print(f"   ✓ has_alcohol: {df['has_alcohol'].sum():,} events with alcohol")
    print(f"   ✓ has_amplified_sound: {df['has_amplified_sound'].sum():,} events with amplified sound")
    print(f"   ✓ has_road_closure: {df['has_road_closure'].sum():,} events with road closure")
    
    # 5. Clean GPS coordinates
    print("\n[5/6] Cleaning GPS coordinates...")
    
    # Count valid coordinates before
    valid_coords_before = df[df['gpslatitude'].notna() & df['gpslongitude'].notna()]
    print(f"   Events with valid GPS: {len(valid_coords_before):,} ({len(valid_coords_before)/len(df)*100:.1f}%)")
    
    # Convert to float (should already be, but ensure)
    df['event_lat'] = pd.to_numeric(df['gpslatitude'], errors='coerce')
    df['event_lon'] = pd.to_numeric(df['gpslongitude'], errors='coerce')
    
    # Validate coordinates are in reasonable range for Austin, TX area
    # Austin is roughly at 30.27°N, 97.74°W
    valid_lat = df['event_lat'].between(29, 32)
    valid_lon = df['event_lon'].between(-99, -96)
    valid_coords = valid_lat & valid_lon
    
    print(f"   Events with valid Austin-area GPS: {valid_coords.sum():,}")
    
    # 6. Create final cleaned dataset
    print("\n[6/6] Creating final dataset...")
    
    # Add derived columns
    df['event_length_days'] = (df['end_date'] - df['start_date']).dt.days + 1
    df['event_year'] = df['start_date'].dt.year
    df['event_month'] = df['start_date'].dt.month
    
    # Select final columns
    final_columns = [
        'folderrsn', 'foldername', 'foldertype', 'status', 'tier_type',
        'start_date', 'end_date', 'event_length', 'event_length_days',
        'event_year', 'event_month',
        'event_lat', 'event_lon',
        'has_alcohol', 'has_amplified_sound', 'has_road_closure',
        'event_applicant_organization', 'road_closure', 'type_of_road_closure'
    ]
    
    # Keep only columns that exist
    final_columns = [c for c in final_columns if c in df.columns]
    df_clean = df[final_columns].copy()
    
    # Rename for clarity
    df_clean = df_clean.rename(columns={
        'folderrsn': 'event_id',
        'foldername': 'event_name',
        'foldertype': 'event_type',
        'event_applicant_organization': 'organizer'
    })
    
    # =========================================
    # VERIFICATION CHECKS
    # =========================================
    print("\n" + "=" * 60)
    print("VERIFICATION CHECKS")
    print("=" * 60)
    
    # Check 1: Table not empty
    assert len(df_clean) > 0, "ERROR: Table is empty!"
    print(f"✓ Table has {len(df_clean):,} rows")
    
    # Check 2: Date range
    valid_dates = df_clean[df_clean['start_date'].notna()]
    if len(valid_dates) > 0:
        print(f"✓ Year range: {valid_dates['event_year'].min():.0f} to {valid_dates['event_year'].max():.0f}")
    
    # Check 3: Events with GPS coordinates
    has_gps = df_clean['event_lat'].notna() & df_clean['event_lon'].notna()
    print(f"✓ Events with GPS: {has_gps.sum():,} ({has_gps.sum()/len(df_clean)*100:.1f}%)")
    
    # Check 4: Status distribution
    print("\n   Status distribution:")
    status_dist = df_clean['status'].value_counts()
    for status, count in status_dist.items():
        print(f"      {status}: {count:,}")
    
    # Check 5: Alcohol events by year
    print("\n   Events with alcohol by year:")
    alcohol_by_year = df_clean[df_clean['has_alcohol']].groupby('event_year').size()
    for year, count in alcohol_by_year.items():
        if pd.notna(year):
            print(f"      {int(year)}: {count:,}")
    
    # =========================================
    # SAMPLE OUTPUT
    # =========================================
    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT (First 5 rows)")
    print("=" * 60)
    sample_cols = ['event_id', 'event_name', 'start_date', 'event_lat', 'event_lon', 'has_alcohol']
    print(df_clean[sample_cols].head(5).to_string(index=False))
    
    # =========================================
    # SAVE
    # =========================================
    output_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\events_cleaned.parquet'
    df_clean.to_parquet(output_path, index=False)
    print(f"\n✓ Saved to: {output_path}")
    
    return df_clean

if __name__ == "__main__":
    df = clean_events()
    print("\n" + "=" * 60)
    print("EVENTS CLEANING COMPLETE!")
    print("=" * 60)
