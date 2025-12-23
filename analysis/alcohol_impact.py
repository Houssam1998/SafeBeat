"""
SafeBeat Analysis - Alcohol Impact Analysis
Deep dive into the impact of alcohol on festival-related incidents

Output:
- datasets/analysis/alcohol_impact_analysis.parquet
- datasets/analysis/alcohol_summary.csv
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
from scipy import stats

def analyze_alcohol_impact():
    print("=" * 60)
    print("ANALYZING ALCOHOL IMPACT ON FESTIVAL INCIDENTS")
    print("=" * 60)
    
    # 1. Load data
    print("\n[1/5] Loading data...")
    
    fact = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\fact_festival_incidents.parquet'
    )
    events_summary = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\events_incident_summary.parquet'
    )
    
    print(f"   Incident-event pairs: {len(fact):,}")
    print(f"   Events with incidents: {len(events_summary):,}")
    
    # 2. Split by alcohol status
    print("\n[2/5] Splitting data by alcohol status...")
    
    alcohol_events = events_summary[events_summary['has_alcohol'] == True]
    no_alcohol_events = events_summary[events_summary['has_alcohol'] == False]
    
    print(f"   Events with alcohol: {len(alcohol_events):,}")
    print(f"   Events without alcohol: {len(no_alcohol_events):,}")
    
    # 3. Calculate statistics
    print("\n[3/5] Calculating comparative statistics...")
    
    results = {}
    
    # Basic counts
    results['alcohol'] = {
        'event_count': len(alcohol_events),
        'total_incidents': alcohol_events['incident_count'].sum(),
        'avg_incidents_per_event': alcohol_events['incident_count'].mean(),
        'median_incidents': alcohol_events['incident_count'].median(),
        'max_incidents': alcohol_events['incident_count'].max(),
        'avg_priority': alcohol_events['avg_priority'].mean(),
        'avg_distance': alcohol_events['avg_distance_km'].mean()
    }
    
    results['no_alcohol'] = {
        'event_count': len(no_alcohol_events),
        'total_incidents': no_alcohol_events['incident_count'].sum(),
        'avg_incidents_per_event': no_alcohol_events['incident_count'].mean(),
        'median_incidents': no_alcohol_events['incident_count'].median(),
        'max_incidents': no_alcohol_events['incident_count'].max(),
        'avg_priority': no_alcohol_events['avg_priority'].mean(),
        'avg_distance': no_alcohol_events['avg_distance_km'].mean()
    }
    
    # 4. Statistical tests
    print("\n[4/5] Performing statistical tests...")
    
    # T-test for incident counts
    if len(alcohol_events) > 1 and len(no_alcohol_events) > 1:
        t_stat, p_value = stats.ttest_ind(
            alcohol_events['incident_count'],
            no_alcohol_events['incident_count']
        )
        results['t_test'] = {'t_statistic': t_stat, 'p_value': p_value}
        
        # Mann-Whitney U test (non-parametric)
        u_stat, u_pvalue = stats.mannwhitneyu(
            alcohol_events['incident_count'],
            no_alcohol_events['incident_count'],
            alternative='two-sided'
        )
        results['mann_whitney'] = {'u_statistic': u_stat, 'p_value': u_pvalue}
    
    # Incident type analysis
    alcohol_incidents = fact[fact['event_has_alcohol'] == True]
    no_alcohol_incidents = fact[fact['event_has_alcohol'] == False]
    
    alcohol_types = alcohol_incidents['final_problem_category'].value_counts().head(10)
    no_alcohol_types = no_alcohol_incidents['final_problem_category'].value_counts().head(10)
    
    results['alcohol_top_types'] = alcohol_types.to_dict()
    results['no_alcohol_top_types'] = no_alcohol_types.to_dict()
    
    # Priority distribution
    alcohol_priority = alcohol_incidents['priority_level'].value_counts().sort_index()
    no_alcohol_priority = no_alcohol_incidents['priority_level'].value_counts().sort_index()
    
    # 5. Save results
    print("\n[5/5] Saving results...")
    
    import os
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis', exist_ok=True)
    
    # Create summary dataframe
    summary_data = {
        'metric': [
            'Event Count',
            'Total Incidents', 
            'Avg Incidents per Event',
            'Median Incidents',
            'Max Incidents',
            'Avg Priority (lower=urgent)',
            'Avg Distance (km)'
        ],
        'with_alcohol': [
            results['alcohol']['event_count'],
            results['alcohol']['total_incidents'],
            results['alcohol']['avg_incidents_per_event'],
            results['alcohol']['median_incidents'],
            results['alcohol']['max_incidents'],
            results['alcohol']['avg_priority'],
            results['alcohol']['avg_distance']
        ],
        'without_alcohol': [
            results['no_alcohol']['event_count'],
            results['no_alcohol']['total_incidents'],
            results['no_alcohol']['avg_incidents_per_event'],
            results['no_alcohol']['median_incidents'],
            results['no_alcohol']['max_incidents'],
            results['no_alcohol']['avg_priority'],
            results['no_alcohol']['avg_distance']
        ]
    }
    
    summary_df = pd.DataFrame(summary_data)
    summary_df['difference'] = summary_df['with_alcohol'] - summary_df['without_alcohol']
    summary_df['pct_difference'] = (summary_df['difference'] / summary_df['without_alcohol'] * 100).round(1)
    
    summary_df.to_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\alcohol_impact_summary.csv', 
        index=False
    )
    
    # Save incident type comparison
    type_comparison = pd.DataFrame({
        'alcohol_type': pd.Series(results['alcohol_top_types']),
        'no_alcohol_type': pd.Series(results['no_alcohol_top_types'])
    })
    type_comparison.to_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\alcohol_incident_types.csv'
    )
    
    # =========================================
    # RESULTS SUMMARY
    # =========================================
    print("\n" + "=" * 60)
    print("ALCOHOL IMPACT ANALYSIS RESULTS")
    print("=" * 60)
    
    print("\n📊 COMPARISON SUMMARY:")
    print(summary_df.to_string(index=False))
    
    if 't_test' in results:
        print(f"\n📈 STATISTICAL SIGNIFICANCE:")
        print(f"   T-test: t = {results['t_test']['t_statistic']:.4f}, p = {results['t_test']['p_value']:.4f}")
        print(f"   Mann-Whitney U: U = {results['mann_whitney']['u_statistic']:.0f}, p = {results['mann_whitney']['p_value']:.4f}")
        
        if results['t_test']['p_value'] < 0.05:
            print("   ✓ The difference is STATISTICALLY SIGNIFICANT (p < 0.05)")
        else:
            print("   ✗ The difference is NOT statistically significant")
    
    print("\n🍺 TOP INCIDENT TYPES AT ALCOHOL EVENTS:")
    for itype, count in list(results['alcohol_top_types'].items())[:5]:
        print(f"   {itype}: {count:,}")
    
    print("\n🚫 TOP INCIDENT TYPES AT NON-ALCOHOL EVENTS:")
    for itype, count in list(results['no_alcohol_top_types'].items())[:5]:
        print(f"   {itype}: {count:,}")
    
    # Key insight
    if results['alcohol']['avg_incidents_per_event'] > results['no_alcohol']['avg_incidents_per_event']:
        pct = ((results['alcohol']['avg_incidents_per_event'] / results['no_alcohol']['avg_incidents_per_event']) - 1) * 100
        print(f"\n🔑 KEY INSIGHT: Alcohol events have {pct:.1f}% more incidents on average")
    
    return results

if __name__ == "__main__":
    results = analyze_alcohol_impact()
    print("\n" + "=" * 60)
    print("ALCOHOL IMPACT ANALYSIS COMPLETE!")
    print("=" * 60)
