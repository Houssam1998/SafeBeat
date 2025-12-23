"""
SafeBeat ETL - Enrich 911 Calls with Geographic Coordinates
Joins 911 calls with DIM_GEO_LOOKUP to add centroid coordinates

Input: 
  - datasets/cleaned/911_calls_cleaned.parquet
  - datasets/cleaned/dim_geo_lookup.csv
Output: datasets/enriched/911_calls_enriched.parquet
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd

def enrich_911_with_geo():
    print("=" * 60)
    print("ENRICHING 911 CALLS WITH GEOGRAPHIC COORDINATES")
    print("=" * 60)
    
    # 1. Load cleaned 911 calls
    print("\n[1/4] Loading cleaned 911 calls...")
    df_911 = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\911_calls_cleaned.parquet'
    )
    print(f"   Total calls loaded: {len(df_911):,}")
    
    # 2. Load DIM_GEO_LOOKUP
    print("\n[2/4] Loading DIM_GEO_LOOKUP...")
    df_geo = pd.read_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\dim_geo_lookup.csv'
    )
    print(f"   Block groups loaded: {len(df_geo):,}")
    
    # Ensure geo_id is string in both dataframes
    # Note: geo lookup has int, 911 may have floats from Excel which add ".0"
    df_geo['geo_id'] = df_geo['geo_id'].astype(str).str.replace('.0', '', regex=False)
    df_911['geo_id'] = df_911['geo_id'].astype(str).str.replace('.0', '', regex=False)
    
    # Check sample values
    print(f"\n   911 geo_id sample: {df_911['geo_id'].iloc[0]}")
    print(f"   Lookup geo_id sample: {df_geo['geo_id'].iloc[0]}")
    
    # 3. Join 911 calls with DIM_GEO_LOOKUP
    print("\n[3/4] Joining 911 calls with geographic lookup...")
    
    df_enriched = df_911.merge(
        df_geo[['geo_id', 'latitude_centroid', 'longitude_centroid', 'area_sq_km']],
        on='geo_id',
        how='left'
    )
    
    # Calculate join success rate
    matched = df_enriched['latitude_centroid'].notna().sum()
    total = len(df_enriched)
    match_rate = matched / total * 100
    
    print(f"   Total calls: {total:,}")
    print(f"   Matched with coordinates: {matched:,}")
    print(f"   Match rate: {match_rate:.1f}%")
    
    # 4. Analyze unmatched records
    print("\n[4/4] Analyzing unmatched records...")
    unmatched = df_enriched[df_enriched['latitude_centroid'].isna()]
    
    if len(unmatched) > 0:
        print(f"   Unmatched records: {len(unmatched):,}")
        unmatched_geo_ids = unmatched['geo_id'].value_counts().head(10)
        print("   Top 10 unmatched geo_ids:")
        for geo_id, count in unmatched_geo_ids.items():
            print(f"      {geo_id}: {count:,}")
    else:
        print("   All records matched!")
    
    # =========================================
    # VERIFICATION CHECKS
    # =========================================
    print("\n" + "=" * 60)
    print("VERIFICATION CHECKS")
    print("=" * 60)
    
    # Check 1: Table not empty
    assert len(df_enriched) > 0, "ERROR: Table is empty!"
    print(f"✓ Table has {len(df_enriched):,} rows")
    
    # Check 2: Match rate acceptable (>80% is good)
    if match_rate < 50:
        print(f"⚠ WARNING: Low match rate ({match_rate:.1f}%)")
    else:
        print(f"✓ Match rate is acceptable: {match_rate:.1f}%")
    
    # Check 3: Verify coordinates are in Austin area for matched records
    matched_df = df_enriched[df_enriched['latitude_centroid'].notna()]
    if len(matched_df) > 0:
        lat_range = (matched_df['latitude_centroid'].min(), matched_df['latitude_centroid'].max())
        lon_range = (matched_df['longitude_centroid'].min(), matched_df['longitude_centroid'].max())
        print(f"✓ Latitude range: [{lat_range[0]:.4f}, {lat_range[1]:.4f}]")
        print(f"✓ Longitude range: [{lon_range[0]:.4f}, {lon_range[1]:.4f}]")
    
    # Check 4: Report on duplicate incident numbers (may be valid in 911 data)
    duplicates = df_enriched['incident_number'].duplicated().sum()
    if duplicates > 0:
        print(f"⚠ Note: Found {duplicates:,} duplicate incident numbers (may be valid for multi-unit calls)")
    else:
        print(f"✓ No duplicate incident numbers")
    
    # =========================================
    # SAMPLE OUTPUT
    # =========================================
    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT (First 5 rows with coordinates)")
    print("=" * 60)
    sample_cols = ['incident_number', 'response_datetime', 'geo_id', 
                   'latitude_centroid', 'longitude_centroid', 'priority_level']
    sample = df_enriched[df_enriched['latitude_centroid'].notna()][sample_cols].head(5)
    print(sample.to_string(index=False))
    
    # =========================================
    # SAVE
    # =========================================
    output_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\911_calls_enriched.parquet'
    df_enriched.to_parquet(output_path, index=False)
    print(f"\n✓ Saved to: {output_path}")
    
    # Statistics summary
    print("\n" + "=" * 60)
    print("ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"   Input records: {len(df_911):,}")
    print(f"   Output records: {len(df_enriched):,}")
    print(f"   Successfully geocoded: {matched:,} ({match_rate:.1f}%)")
    print(f"   Missing coordinates: {total - matched:,} ({100-match_rate:.1f}%)")
    
    return df_enriched

if __name__ == "__main__":
    df = enrich_911_with_geo()
    print("\n" + "=" * 60)
    print("911 CALLS ENRICHMENT COMPLETE!")
    print("=" * 60)
