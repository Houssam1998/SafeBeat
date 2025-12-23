"""
SafeBeat ETL - DIM_GEO_LOOKUP Table Creation
Creates a lookup table mapping Geo IDs to centroid coordinates

Output: datasets/cleaned/dim_geo_lookup.csv
Columns: geo_id, latitude_centroid, longitude_centroid, area_sq_km
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import geopandas as gpd
import pandas as pd

def create_dim_geo_lookup():
    print("=" * 60)
    print("CREATING DIM_GEO_LOOKUP TABLE")
    print("=" * 60)
    
    # 1. Load the shapefile (Texas Block Groups)
    print("\n[1/5] Loading shapefile...")
    gdf = gpd.read_file(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\tl_2024_48_bg\tl_2024_48_bg.shp'
    )
    print(f"   Total Texas block groups loaded: {len(gdf):,}")
    
    # 2. Filter for Travis County (COUNTYFP = '453')
    print("\n[2/5] Filtering for Travis County (COUNTYFP = '453')...")
    gdf_travis = gdf[gdf['COUNTYFP'] == '453'].copy()
    print(f"   Travis County block groups: {len(gdf_travis):,}")
    
    if len(gdf_travis) == 0:
        print("   WARNING: No data found for Travis County!")
        print("   Available counties:", gdf['COUNTYFP'].unique()[:20])
        return None
    
    # 3. Extract centroid coordinates (already available as INTPTLAT, INTPTLON)
    print("\n[3/5] Extracting centroid coordinates...")
    # Convert INTPTLAT and INTPTLON from strings to floats
    gdf_travis['latitude_centroid'] = gdf_travis['INTPTLAT'].astype(float)
    gdf_travis['longitude_centroid'] = gdf_travis['INTPTLON'].astype(float)
    
    # 4. Calculate area in km² (ALAND is in m²)
    print("\n[4/5] Calculating area in km²...")
    gdf_travis['area_sq_km'] = gdf_travis['ALAND'] / 1_000_000
    
    # 5. Create the final lookup table
    print("\n[5/5] Creating final lookup table...")
    dim_geo_lookup = gdf_travis[['GEOID', 'latitude_centroid', 'longitude_centroid', 'area_sq_km']].copy()
    dim_geo_lookup.columns = ['geo_id', 'latitude_centroid', 'longitude_centroid', 'area_sq_km']
    
    # =========================================
    # VERIFICATION CHECKS
    # =========================================
    print("\n" + "=" * 60)
    print("VERIFICATION CHECKS")
    print("=" * 60)
    
    # Check 1: Table not empty
    assert len(dim_geo_lookup) > 0, "ERROR: Table is empty!"
    print(f"✓ Table has {len(dim_geo_lookup):,} rows")
    
    # Check 2: No duplicate geo_ids
    duplicates = dim_geo_lookup['geo_id'].duplicated().sum()
    assert duplicates == 0, f"ERROR: Found {duplicates} duplicate geo_ids!"
    print(f"✓ No duplicate geo_ids found")
    
    # Check 3: Latitude in Texas range (roughly 25.8° to 36.5°)
    lat_min, lat_max = dim_geo_lookup['latitude_centroid'].min(), dim_geo_lookup['latitude_centroid'].max()
    assert 25 < lat_min and lat_max < 37, f"ERROR: Latitude out of range: [{lat_min}, {lat_max}]"
    print(f"✓ Latitude range: [{lat_min:.4f}, {lat_max:.4f}] (within Texas)")
    
    # Check 4: Longitude in Texas range (roughly -106.6° to -93.5°)
    lon_min, lon_max = dim_geo_lookup['longitude_centroid'].min(), dim_geo_lookup['longitude_centroid'].max()
    assert -107 < lon_min and lon_max < -93, f"ERROR: Longitude out of range: [{lon_min}, {lon_max}]"
    print(f"✓ Longitude range: [{lon_min:.4f}, {lon_max:.4f}] (within Texas)")
    
    # Check 5: Area values are positive
    assert (dim_geo_lookup['area_sq_km'] > 0).all(), "ERROR: Some areas are <= 0!"
    print(f"✓ All area values are positive (min: {dim_geo_lookup['area_sq_km'].min():.4f} km²)")
    
    # Check 6: geo_id format (should be 12 digits)
    geo_id_lengths = dim_geo_lookup['geo_id'].str.len().unique()
    print(f"✓ Geo ID lengths: {geo_id_lengths.tolist()}")
    
    # =========================================
    # SAMPLE OUTPUT
    # =========================================
    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT (First 10 rows)")
    print("=" * 60)
    print(dim_geo_lookup.head(10).to_string(index=False))
    
    # =========================================
    # SAVE TO CSV
    # =========================================
    output_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\dim_geo_lookup.csv'
    dim_geo_lookup.to_csv(output_path, index=False)
    print(f"\n✓ Saved to: {output_path}")
    
    # Statistics
    print("\n" + "=" * 60)
    print("STATISTICS")
    print("=" * 60)
    print(dim_geo_lookup.describe())
    
    return dim_geo_lookup

if __name__ == "__main__":
    df = create_dim_geo_lookup()
    print("\n" + "=" * 60)
    print("DIM_GEO_LOOKUP CREATION COMPLETE!")
    print("=" * 60)
