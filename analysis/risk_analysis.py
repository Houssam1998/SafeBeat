"""
SafeBeat Analysis - Risk Scoring & Heatmaps
Creates risk scores for festivals and generates analysis outputs

Output:
- datasets/analysis/festival_risk_scores.parquet
- datasets/analysis/risk_heatmap_data.parquet
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np

def calculate_risk_scores():
    print("=" * 60)
    print("CALCULATING FESTIVAL RISK SCORES")
    print("=" * 60)
    
    # 1. Load data
    print("\n[1/6] Loading data...")
    
    fact = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\fact_festival_incidents.parquet'
    )
    events = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\events_cleaned.parquet'
    )
    
    print(f"   Fact table: {len(fact):,} incident-event pairs")
    print(f"   Events: {len(events):,}")
    
    # 2. Calculate incident metrics per festival
    print("\n[2/6] Calculating incident metrics per festival...")
    
    # Aggregate incidents by event
    festival_metrics = fact.groupby(['event_id', 'event_name']).agg({
        'incident_number': 'count',
        'priority_numeric': ['mean', 'min'],
        'distance_km': 'mean',
        'response_time': 'mean',
        'event_has_alcohol': 'first',
        'event_has_amplified_sound': 'first'
    }).reset_index()
    
    # Flatten column names
    festival_metrics.columns = [
        'event_id', 'event_name', 'incident_count', 
        'avg_priority', 'min_priority', 'avg_distance', 
        'avg_response_time', 'has_alcohol', 'has_amplified_sound'
    ]
    
    print(f"   Festivals with incidents: {len(festival_metrics):,}")
    
    # 3. Calculate priority breakdown
    print("\n[3/6] Calculating priority breakdown...")
    
    priority_breakdown = fact.groupby(['event_id', 'priority_level']).size().unstack(fill_value=0)
    priority_breakdown.columns = [f'count_{col.replace(" ", "_").lower()}' for col in priority_breakdown.columns]
    priority_breakdown = priority_breakdown.reset_index()
    
    festival_metrics = festival_metrics.merge(priority_breakdown, on='event_id', how='left')
    
    # 4. Calculate incident type breakdown
    print("\n[4/6] Analyzing incident types...")
    
    # Top incident type per festival
    top_incident_type = fact.groupby('event_id')['final_problem_category'].agg(
        lambda x: x.value_counts().index[0] if len(x) > 0 else 'Unknown'
    ).reset_index()
    top_incident_type.columns = ['event_id', 'top_incident_type']
    
    festival_metrics = festival_metrics.merge(top_incident_type, on='event_id', how='left')
    
    # 5. Calculate RISK SCORE
    print("\n[5/6] Calculating Risk Scores...")
    
    # Risk score formula:
    # - Base: incident count (normalized)
    # - Priority weight: more high-priority = higher risk
    # - Distance weight: closer incidents = higher risk
    # - Alcohol bonus: +20% if alcohol served
    
    # Normalize incident count (0-100 scale)
    max_incidents = festival_metrics['incident_count'].max()
    festival_metrics['incident_score'] = (festival_metrics['incident_count'] / max_incidents) * 100
    
    # Priority score (Priority 0 = 4 points, Priority 1 = 3, etc.)
    if 'count_priority_0' in festival_metrics.columns:
        festival_metrics['priority_score'] = (
            festival_metrics.get('count_priority_0', 0) * 4 +
            festival_metrics.get('count_priority_1', 0) * 3 +
            festival_metrics.get('count_priority_2', 0) * 2 +
            festival_metrics.get('count_priority_3', 0) * 1
        )
    else:
        festival_metrics['priority_score'] = 0
    
    # Normalize priority score
    max_priority_score = festival_metrics['priority_score'].max()
    if max_priority_score > 0:
        festival_metrics['priority_score_norm'] = (festival_metrics['priority_score'] / max_priority_score) * 100
    else:
        festival_metrics['priority_score_norm'] = 0
    
    # Distance score (closer = higher risk, inverted)
    festival_metrics['distance_score'] = (1 - festival_metrics['avg_distance'] / 1.5) * 100
    festival_metrics['distance_score'] = festival_metrics['distance_score'].clip(0, 100)
    
    # Combined risk score
    festival_metrics['risk_score'] = (
        festival_metrics['incident_score'] * 0.4 +
        festival_metrics['priority_score_norm'] * 0.35 +
        festival_metrics['distance_score'] * 0.15 +
        (festival_metrics['has_alcohol'].astype(int) * 10)  # Alcohol bonus
    )
    
    # Risk category
    def categorize_risk(score):
        if score >= 70:
            return 'HIGH'
        elif score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    festival_metrics['risk_category'] = festival_metrics['risk_score'].apply(categorize_risk)
    
    # 6. Save results
    print("\n[6/6] Saving results...")
    
    # Sort by risk score descending
    festival_metrics = festival_metrics.sort_values('risk_score', ascending=False)
    
    # Create output directory
    import os
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis', exist_ok=True)
    
    output_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\festival_risk_scores.parquet'
    festival_metrics.to_parquet(output_path, index=False)
    print(f"   ✓ Saved to: {output_path}")
    
    # Also save as CSV for easy viewing
    csv_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\festival_risk_scores.csv'
    festival_metrics.to_csv(csv_path, index=False)
    print(f"   ✓ CSV saved to: {csv_path}")
    
    # =========================================
    # RESULTS SUMMARY
    # =========================================
    print("\n" + "=" * 60)
    print("RISK ANALYSIS RESULTS")
    print("=" * 60)
    
    print("\n📊 Risk Category Distribution:")
    risk_dist = festival_metrics['risk_category'].value_counts()
    for cat, count in risk_dist.items():
        print(f"   {cat}: {count} festivals")
    
    print("\n🔴 TOP 10 HIGHEST RISK FESTIVALS:")
    top_risk = festival_metrics.head(10)[['event_name', 'incident_count', 'risk_score', 'risk_category', 'has_alcohol']]
    print(top_risk.to_string(index=False))
    
    print("\n📈 Risk Score Statistics:")
    print(f"   Mean: {festival_metrics['risk_score'].mean():.1f}")
    print(f"   Median: {festival_metrics['risk_score'].median():.1f}")
    print(f"   Max: {festival_metrics['risk_score'].max():.1f}")
    print(f"   Min: {festival_metrics['risk_score'].min():.1f}")
    
    return festival_metrics

if __name__ == "__main__":
    df = calculate_risk_scores()
    print("\n" + "=" * 60)
    print("RISK ANALYSIS COMPLETE!")
    print("=" * 60)
