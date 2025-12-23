"""
SafeBeat - Data Exploration Script
This script explores all datasets to understand their structure
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import geopandas as gpd

results = []

results.append("=" * 60)
results.append("1. EXPLORING 911 CALLS DATA")
results.append("=" * 60)

df_911 = pd.read_excel(
    r'd:\uemf\s9\Data mining\SafeBeat\datasets\APD_911_Calls_for_Service_2023-2025_20251223.xlsx',
    nrows=1000
)
results.append(f"\nColumns: {df_911.columns.tolist()}")
results.append(f"\nData Types:\n{df_911.dtypes}")
results.append(f"\nSample (first 3 rows):\n{df_911.head(3).to_string()}")

# Check Geo ID format
if 'Geo ID' in df_911.columns:
    results.append(f"\n\nGeo ID sample values:\n{df_911['Geo ID'].head(10).to_string()}")
    results.append(f"Geo ID type: {df_911['Geo ID'].dtype}")

results.append("\n" + "=" * 60)
results.append("2. EXPLORING EVENTS DATA")
results.append("=" * 60)

df_events = pd.read_excel(
    r'd:\uemf\s9\Data mining\SafeBeat\datasets\ACE_Events_20251223.xlsx',
    nrows=100
)
results.append(f"\nColumns: {df_events.columns.tolist()}")
results.append(f"\nData Types:\n{df_events.dtypes}")
results.append(f"\nSample (first 3 rows):\n{df_events.head(3).to_string()}")

results.append("\n" + "=" * 60)
results.append("3. EXPLORING FOURSQUARE DATA")
results.append("=" * 60)

df_fsq = pd.read_parquet(
    r'd:\uemf\s9\Data mining\SafeBeat\datasets\Foursquare_train.parquet'
)
results.append(f"\nColumns: {df_fsq.columns.tolist()}")
results.append(f"\nData Types:\n{df_fsq.dtypes}")
results.append(f"\nSample (first 5 rows):\n{df_fsq.head(5).to_string()}")
results.append(f"\nShape: {df_fsq.shape}")

results.append("\n" + "=" * 60)
results.append("4. EXPLORING SHAPEFILE (GEO LOOKUP)")
results.append("=" * 60)

gdf = gpd.read_file(
    r'd:\uemf\s9\Data mining\SafeBeat\datasets\tl_2024_48_bg\tl_2024_48_bg.shp',
    rows=100
)
results.append(f"\nColumns: {gdf.columns.tolist()}")
results.append(f"\nData Types:\n{gdf.dtypes}")
results.append(f"\nSample (first 3 rows, excluding geometry):\n{gdf.drop(columns=['geometry']).head(3).to_string()}")

# Check GEOID format
if 'GEOID' in gdf.columns:
    results.append(f"\n\nGEOID sample values:\n{gdf['GEOID'].head(10).to_string()}")
    results.append(f"GEOID type: {gdf['GEOID'].dtype}")

# Check CRS
results.append(f"\n\nCRS (Coordinate Reference System): {gdf.crs}")

# Check for Travis County (COUNTYFP == '453')
if 'COUNTYFP' in gdf.columns:
    results.append(f"\n\nCounty codes in sample: {gdf['COUNTYFP'].unique()[:10].tolist()}")

results.append("\n" + "=" * 60)
results.append("EXPLORATION COMPLETE")
results.append("=" * 60)

# Write to file
with open('exploration_results.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(results))

print("Results saved to exploration_results.txt")
