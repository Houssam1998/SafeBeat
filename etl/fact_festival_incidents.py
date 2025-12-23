"""
SafeBeat ETL - Create FACT_FESTIVAL_INCIDENTS
Performs spatio-temporal join between 911 calls and events

This script:
1. Joins 911 calls with events based on TIME (incident during event dates)
2. Filters by SPACE (incident within 1.5km of event location)
3. Calculates Haversine distances
4. Creates the main fact table for analysis

Output: datasets/enriched/fact_festival_incidents.parquet
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """
    # Convert to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    # Earth radius in kilometers
    r = 6371
    
    return c * r

def haversine_vectorized(lon1, lat1, lon2, lat2):
    """Vectorized Haversine for pandas operations"""
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return 6371 * c

def create_fact_festival_incidents():
    print("=" * 60)
    print("CREATING FACT_FESTIVAL_INCIDENTS TABLE")
    print("=" * 60)
    
    # 1. Load enriched 911 calls
    print("\n[1/6] Loading enriched 911 calls...")
    df_911 = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\911_calls_enriched.parquet'
    )
    print(f"   Total calls: {len(df_911):,}")
    
    # Filter to only calls with coordinates
    df_911 = df_911[df_911['latitude_centroid'].notna()].copy()
    print(f"   Calls with coordinates: {len(df_911):,}")
    
    # 2. Load cleaned events
    print("\n[2/6] Loading cleaned events...")
    df_events = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\events_cleaned.parquet'
    )
    print(f"   Total events: {len(df_events):,}")
    
    # Filter to events with valid GPS and dates
    df_events = df_events[
        (df_events['event_lat'].notna()) & 
        (df_events['event_lon'].notna()) &
        (df_events['start_date'].notna()) &
        (df_events['end_date'].notna())
    ].copy()
    print(f"   Events with GPS + dates: {len(df_events):,}")
    
    # Remove duplicate events (same event_id)
    df_events = df_events.drop_duplicates(subset=['event_id'], keep='first')
    print(f"   Unique events: {len(df_events):,}")
    
    # 3. Prepare data for join
    print("\n[3/6] Preparing data for spatio-temporal join...")
    
    # Ensure datetime columns are datetime type
    df_911['response_datetime'] = pd.to_datetime(df_911['response_datetime'])
    df_events['start_date'] = pd.to_datetime(df_events['start_date'])
    df_events['end_date'] = pd.to_datetime(df_events['end_date'])
    
    # Get date range overlap
    calls_min = df_911['response_datetime'].min()
    calls_max = df_911['response_datetime'].max()
    events_min = df_events['start_date'].min()
    events_max = df_events['end_date'].max()
    
    print(f"   911 Calls range: {calls_min.date()} to {calls_max.date()}")
    print(f"   Events range: {events_min.date()} to {events_max.date()}")
    
    # Filter events to only those overlapping with 911 calls timeframe
    df_events_filtered = df_events[
        (df_events['end_date'] >= calls_min) &
        (df_events['start_date'] <= calls_max)
    ].copy()
    print(f"   Events in 911 calls timeframe: {len(df_events_filtered):,}")
    
    # 4. Perform spatio-temporal join
    print("\n[4/6] Performing spatio-temporal join...")
    print("   (This may take a while for large datasets...)")
    
    # Strategy: For each event, find incidents that:
    # - Occurred during the event dates
    # - Are within 1.5 km of the event location
    
    results = []
    total_events = len(df_events_filtered)
    DISTANCE_THRESHOLD_KM = 1.5
    
    for idx, (_, event) in enumerate(df_events_filtered.iterrows()):
        if idx % 100 == 0:
            print(f"   Processing event {idx+1}/{total_events}...", end='\r')
        
        # Temporal filter: incidents during event
        temporal_mask = (
            (df_911['response_datetime'] >= event['start_date']) &
            (df_911['response_datetime'] <= event['end_date'])
        )
        
        if temporal_mask.sum() == 0:
            continue
            
        incidents_during_event = df_911[temporal_mask].copy()
        
        # Calculate distance for each incident
        incidents_during_event['distance_km'] = haversine_vectorized(
            incidents_during_event['longitude_centroid'].values,
            incidents_during_event['latitude_centroid'].values,
            event['event_lon'],
            event['event_lat']
        )
        
        # Spatial filter: within threshold
        nearby_incidents = incidents_during_event[
            incidents_during_event['distance_km'] <= DISTANCE_THRESHOLD_KM
        ].copy()
        
        if len(nearby_incidents) > 0:
            # Add event info to each matched incident
            nearby_incidents['event_id'] = event['event_id']
            nearby_incidents['event_name'] = event['event_name']
            nearby_incidents['event_start_date'] = event['start_date']
            nearby_incidents['event_end_date'] = event['end_date']
            nearby_incidents['event_has_alcohol'] = event.get('has_alcohol', False)
            nearby_incidents['event_has_amplified_sound'] = event.get('has_amplified_sound', False)
            
            results.append(nearby_incidents)
    
    print(f"\n   Completed processing {total_events} events")
    
    # 5. Combine results
    print("\n[5/6] Combining results...")
    
    if len(results) == 0:
        print("   ⚠ WARNING: No matching incidents found!")
        print("   This could mean:")
        print("      - Events and 911 calls don't overlap temporally")
        print("      - Events GPS locations don't match 911 call areas")
        return None
    
    df_fact = pd.concat(results, ignore_index=True)
    print(f"   Total incident-event pairs: {len(df_fact):,}")
    
    # Select relevant columns for fact table
    fact_columns = [
        # Incident info
        'incident_number', 'incident_type', 'priority_level', 'priority_numeric',
        'response_datetime', 'response_time', 'sector',
        'latitude_centroid', 'longitude_centroid',
        'initial_problem_category', 'final_problem_category',
        # Event info
        'event_id', 'event_name', 'event_start_date', 'event_end_date',
        'event_has_alcohol', 'event_has_amplified_sound',
        # Calculated
        'distance_km'
    ]
    
    # Keep only columns that exist
    fact_columns = [c for c in fact_columns if c in df_fact.columns]
    df_fact_final = df_fact[fact_columns].copy()
    
    # =========================================
    # VERIFICATION CHECKS
    # =========================================
    print("\n" + "=" * 60)
    print("VERIFICATION CHECKS")
    print("=" * 60)
    
    print(f"✓ Total incident-event pairs: {len(df_fact_final):,}")
    print(f"✓ Unique incidents: {df_fact_final['incident_number'].nunique():,}")
    print(f"✓ Unique events: {df_fact_final['event_id'].nunique():,}")
    
    # Distance statistics
    print(f"\n   Distance statistics (km):")
    print(f"      Min: {df_fact_final['distance_km'].min():.3f}")
    print(f"      Max: {df_fact_final['distance_km'].max():.3f}")
    print(f"      Mean: {df_fact_final['distance_km'].mean():.3f}")
    print(f"      Median: {df_fact_final['distance_km'].median():.3f}")
    
    # Priority distribution
    print(f"\n   Priority distribution:")
    priority_dist = df_fact_final['priority_level'].value_counts()
    for priority, count in priority_dist.items():
        print(f"      {priority}: {count:,}")
    
    # Alcohol vs no alcohol
    print(f"\n   Incidents by alcohol status:")
    alcohol_dist = df_fact_final.groupby('event_has_alcohol').size()
    print(f"      With alcohol: {alcohol_dist.get(True, 0):,}")
    print(f"      Without alcohol: {alcohol_dist.get(False, 0):,}")
    
    # =========================================
    # SAMPLE OUTPUT
    # =========================================
    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT (First 5 rows)")
    print("=" * 60)
    sample_cols = ['incident_number', 'event_name', 'distance_km', 'priority_level', 'response_datetime']
    sample_cols = [c for c in sample_cols if c in df_fact_final.columns]
    print(df_fact_final[sample_cols].head(5).to_string(index=False))
    
    # =========================================
    # SAVE
    # =========================================
    print("\n" + "=" * 60)
    print("SAVING OUTPUT")
    print("=" * 60)
    
    output_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\fact_festival_incidents.parquet'
    df_fact_final.to_parquet(output_path, index=False)
    print(f"✓ Saved to: {output_path}")
    
    # Also save summary aggregations
    print("\n[6/6] Creating summary tables...")
    
    # Incidents per event
    events_summary = df_fact_final.groupby(['event_id', 'event_name']).agg({
        'incident_number': 'count',
        'priority_numeric': 'mean',
        'distance_km': 'mean',
        'event_has_alcohol': 'first',
        'event_has_amplified_sound': 'first'
    }).reset_index()
    events_summary.columns = ['event_id', 'event_name', 'incident_count', 
                               'avg_priority', 'avg_distance_km', 
                               'has_alcohol', 'has_amplified_sound']
    events_summary = events_summary.sort_values('incident_count', ascending=False)
    
    summary_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\events_incident_summary.parquet'
    events_summary.to_parquet(summary_path, index=False)
    print(f"✓ Summary saved to: {summary_path}")
    
    print("\n   Top 10 events by incident count:")
    print(events_summary.head(10).to_string(index=False))
    
    return df_fact_final

if __name__ == "__main__":
    df = create_fact_festival_incidents()
    print("\n" + "=" * 60)
    print("FACT_FESTIVAL_INCIDENTS CREATION COMPLETE!")
    print("=" * 60)
