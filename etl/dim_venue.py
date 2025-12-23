"""
SafeBeat ETL - DIM_VENUE Table Creation
Creates a venue dimension table from Foursquare data

Output: datasets/cleaned/dim_venue.csv
Columns: venue_id, name, latitude, longitude, address, city, state, country, categories
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd

def create_dim_venue():
    print("=" * 60)
    print("CREATING DIM_VENUE TABLE")
    print("=" * 60)
    
    # 1. Load Foursquare data
    print("\n[1/5] Loading Foursquare parquet data...")
    df = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\Foursquare_train.parquet'
    )
    print(f"   Total venues loaded: {len(df):,}")
    print(f"   Columns: {df.columns.tolist()}")
    
    # 2. Select and rename relevant columns
    print("\n[2/5] Selecting relevant columns...")
    dim_venue = df[[
        'id', 'name', 'latitude', 'longitude', 
        'address', 'city', 'state', 'country', 
        'categories', 'point_of_interest'
    ]].copy()
    
    dim_venue.columns = [
        'venue_id', 'name', 'latitude', 'longitude',
        'address', 'city', 'state', 'country',
        'category', 'poi_id'
    ]
    
    # 3. Clean data
    print("\n[3/5] Cleaning data...")
    
    # Count nulls before cleaning
    print("   Null counts before cleaning:")
    null_counts = dim_venue.isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            print(f"      {col}: {count:,} ({count/len(dim_venue)*100:.1f}%)")
    
    # Fill missing values with appropriate defaults
    dim_venue['address'] = dim_venue['address'].fillna('')
    dim_venue['city'] = dim_venue['city'].fillna('')
    dim_venue['state'] = dim_venue['state'].fillna('')
    dim_venue['category'] = dim_venue['category'].fillna('Unknown')
    
    # Remove venues without coordinates (essential for spatial analysis)
    before_count = len(dim_venue)
    dim_venue = dim_venue.dropna(subset=['latitude', 'longitude'])
    dropped_count = before_count - len(dim_venue)
    print(f"   Dropped {dropped_count:,} venues without coordinates")
    
    # 4. Add derived columns
    print("\n[4/5] Adding derived columns...")
    
    # Extract primary category (first category if multiple)
    dim_venue['primary_category'] = dim_venue['category'].apply(
        lambda x: x.split(',')[0].strip() if isinstance(x, str) and x else 'Unknown'
    )
    
    # Country distribution
    print("\n   Top 10 countries:")
    country_dist = dim_venue['country'].value_counts().head(10)
    for country, count in country_dist.items():
        print(f"      {country}: {count:,}")
    
    # Category distribution
    print("\n   Top 10 categories:")
    cat_dist = dim_venue['primary_category'].value_counts().head(10)
    for cat, count in cat_dist.items():
        print(f"      {cat}: {count:,}")
    
    # =========================================
    # VERIFICATION CHECKS
    # =========================================
    print("\n" + "=" * 60)
    print("VERIFICATION CHECKS")
    print("=" * 60)
    
    # Check 1: Table not empty
    assert len(dim_venue) > 0, "ERROR: Table is empty!"
    print(f"✓ Table has {len(dim_venue):,} rows")
    
    # Check 2: No duplicate venue_ids
    duplicates = dim_venue['venue_id'].duplicated().sum()
    if duplicates > 0:
        print(f"⚠ Found {duplicates:,} duplicate venue_ids - removing...")
        dim_venue = dim_venue.drop_duplicates(subset=['venue_id'], keep='first')
    print(f"✓ Unique venues: {len(dim_venue):,}")
    
    # Check 3: Latitude in valid range (-90 to 90)
    lat_min, lat_max = dim_venue['latitude'].min(), dim_venue['latitude'].max()
    assert -90 <= lat_min and lat_max <= 90, f"ERROR: Latitude out of range: [{lat_min}, {lat_max}]"
    print(f"✓ Latitude range: [{lat_min:.4f}, {lat_max:.4f}]")
    
    # Check 4: Longitude in valid range (-180 to 180)
    lon_min, lon_max = dim_venue['longitude'].min(), dim_venue['longitude'].max()
    assert -180 <= lon_min and lon_max <= 180, f"ERROR: Longitude out of range: [{lon_min}, {lon_max}]"
    print(f"✓ Longitude range: [{lon_min:.4f}, {lon_max:.4f}]")
    
    # Check 5: All required columns present
    required_cols = ['venue_id', 'name', 'latitude', 'longitude', 'category']
    for col in required_cols:
        assert col in dim_venue.columns, f"ERROR: Missing column {col}"
    print(f"✓ All required columns present")
    
    # =========================================
    # SAMPLE OUTPUT
    # =========================================
    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT (First 5 rows)")
    print("=" * 60)
    sample_cols = ['venue_id', 'name', 'latitude', 'longitude', 'city', 'country', 'primary_category']
    print(dim_venue[sample_cols].head(5).to_string(index=False))
    
    # =========================================
    # SAVE TO CSV
    # =========================================
    output_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\dim_venue.csv'
    dim_venue.to_csv(output_path, index=False)
    print(f"\n✓ Saved to: {output_path}")
    
    # Also save as parquet for efficiency
    output_path_parquet = r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\dim_venue.parquet'
    dim_venue.to_parquet(output_path_parquet, index=False)
    print(f"✓ Also saved to: {output_path_parquet}")
    
    return dim_venue

if __name__ == "__main__":
    df = create_dim_venue()
    print("\n" + "=" * 60)
    print("DIM_VENUE CREATION COMPLETE!")
    print("=" * 60)
