"""
SafeBeat Analysis - Response Time Analysis
Analyzes 911 response time patterns during festivals

Output:
- datasets/analysis/response_time_analysis.parquet
- datasets/analysis/response_time_summary.csv
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
from scipy import stats

def analyze_response_times():
    print("=" * 60)
    print("ANALYZING RESPONSE TIME PATTERNS")
    print("=" * 60)
    
    # 1. Load data
    print("\n[1/5] Loading data...")
    
    df_911 = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\911_calls_enriched.parquet'
    )
    fact = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\fact_festival_incidents.parquet'
    )
    
    print(f"   Total 911 calls: {len(df_911):,}")
    print(f"   Festival-linked incidents: {len(fact):,}")
    
    # 2. Compare festival vs non-festival response times
    print("\n[2/5] Comparing festival vs non-festival response times...")
    
    # Get incident numbers that are linked to festivals
    festival_incident_ids = set(fact['incident_number'].unique())
    
    df_911['is_festival_related'] = df_911['incident_number'].isin(festival_incident_ids)
    
    festival_calls = df_911[df_911['is_festival_related']]
    non_festival_calls = df_911[~df_911['is_festival_related']]
    
    # Filter extreme outliers (response time > 2 hours)
    festival_calls = festival_calls[festival_calls['response_time'] < 7200]
    non_festival_calls = non_festival_calls[non_festival_calls['response_time'] < 7200]
    
    print(f"   Festival calls (filtered): {len(festival_calls):,}")
    print(f"   Non-festival calls (filtered): {len(non_festival_calls):,}")
    
    results = {}
    
    # Basic statistics
    results['festival'] = {
        'count': len(festival_calls),
        'mean_seconds': festival_calls['response_time'].mean(),
        'median_seconds': festival_calls['response_time'].median(),
        'std_seconds': festival_calls['response_time'].std(),
        'min_seconds': festival_calls['response_time'].min(),
        'max_seconds': festival_calls['response_time'].max(),
        'p25_seconds': festival_calls['response_time'].quantile(0.25),
        'p75_seconds': festival_calls['response_time'].quantile(0.75)
    }
    
    results['non_festival'] = {
        'count': len(non_festival_calls),
        'mean_seconds': non_festival_calls['response_time'].mean(),
        'median_seconds': non_festival_calls['response_time'].median(),
        'std_seconds': non_festival_calls['response_time'].std(),
        'min_seconds': non_festival_calls['response_time'].min(),
        'max_seconds': non_festival_calls['response_time'].max(),
        'p25_seconds': non_festival_calls['response_time'].quantile(0.25),
        'p75_seconds': non_festival_calls['response_time'].quantile(0.75)
    }
    
    # 3. Response time by priority
    print("\n[3/5] Analyzing response time by priority...")
    
    priority_response = df_911.groupby(['is_festival_related', 'priority_level']).agg({
        'response_time': ['mean', 'median', 'count']
    }).reset_index()
    priority_response.columns = ['is_festival', 'priority_level', 'mean_time', 'median_time', 'count']
    
    results['by_priority'] = priority_response.to_dict('records')
    
    # 4. Response time by hour of day
    print("\n[4/5] Analyzing response time by hour...")
    
    hourly_response = df_911.groupby(['is_festival_related', 'response_hour']).agg({
        'response_time': 'mean'
    }).reset_index()
    hourly_response.columns = ['is_festival', 'hour', 'mean_response_time']
    
    results['by_hour'] = hourly_response.to_dict('records')
    
    # Statistical test
    if len(festival_calls) > 10 and len(non_festival_calls) > 10:
        t_stat, p_value = stats.ttest_ind(
            festival_calls['response_time'].dropna(),
            non_festival_calls['response_time'].dropna()
        )
        results['t_test'] = {'t_statistic': t_stat, 'p_value': p_value}
    
    # 5. Save results
    print("\n[5/5] Saving results...")
    
    import os
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis', exist_ok=True)
    
    # Summary dataframe
    summary_data = {
        'metric': ['Count', 'Mean (sec)', 'Mean (min)', 'Median (sec)', 'Median (min)', 
                   'Std Dev', '25th Percentile', '75th Percentile'],
        'festival': [
            results['festival']['count'],
            results['festival']['mean_seconds'],
            results['festival']['mean_seconds'] / 60,
            results['festival']['median_seconds'],
            results['festival']['median_seconds'] / 60,
            results['festival']['std_seconds'],
            results['festival']['p25_seconds'],
            results['festival']['p75_seconds']
        ],
        'non_festival': [
            results['non_festival']['count'],
            results['non_festival']['mean_seconds'],
            results['non_festival']['mean_seconds'] / 60,
            results['non_festival']['median_seconds'],
            results['non_festival']['median_seconds'] / 60,
            results['non_festival']['std_seconds'],
            results['non_festival']['p25_seconds'],
            results['non_festival']['p75_seconds']
        ]
    }
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\response_time_summary.csv',
        index=False
    )
    
    priority_response.to_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\response_time_by_priority.csv',
        index=False
    )
    
    # =========================================
    # RESULTS SUMMARY
    # =========================================
    print("\n" + "=" * 60)
    print("RESPONSE TIME ANALYSIS RESULTS")
    print("=" * 60)
    
    print("\n⏱️ RESPONSE TIME COMPARISON:")
    print(f"\n   {'Metric':<20} {'Festival':>15} {'Non-Festival':>15} {'Diff':>10}")
    print("   " + "-" * 60)
    print(f"   {'Mean (minutes)':<20} {results['festival']['mean_seconds']/60:>15.1f} {results['non_festival']['mean_seconds']/60:>15.1f} {(results['festival']['mean_seconds']-results['non_festival']['mean_seconds'])/60:>+10.1f}")
    print(f"   {'Median (minutes)':<20} {results['festival']['median_seconds']/60:>15.1f} {results['non_festival']['median_seconds']/60:>15.1f} {(results['festival']['median_seconds']-results['non_festival']['median_seconds'])/60:>+10.1f}")
    
    if 't_test' in results:
        print(f"\n📈 STATISTICAL TEST:")
        print(f"   T-test: t = {results['t_test']['t_statistic']:.4f}, p = {results['t_test']['p_value']:.6f}")
        if results['t_test']['p_value'] < 0.05:
            print("   ✓ The difference is STATISTICALLY SIGNIFICANT")
        else:
            print("   ✗ The difference is NOT statistically significant")
    
    print("\n🚨 RESPONSE TIME BY PRIORITY:")
    print(priority_response.to_string(index=False))
    
    # Key insight
    diff_minutes = (results['festival']['mean_seconds'] - results['non_festival']['mean_seconds']) / 60
    if diff_minutes > 0:
        print(f"\n🔑 KEY INSIGHT: Festival incidents have {diff_minutes:.1f} minutes SLOWER response on average")
    else:
        print(f"\n🔑 KEY INSIGHT: Festival incidents have {abs(diff_minutes):.1f} minutes FASTER response on average")
    
    return results

if __name__ == "__main__":
    results = analyze_response_times()
    print("\n" + "=" * 60)
    print("RESPONSE TIME ANALYSIS COMPLETE!")
    print("=" * 60)
