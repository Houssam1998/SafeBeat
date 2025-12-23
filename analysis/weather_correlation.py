"""
SafeBeat Analysis - Weather Correlation
Analyzes the relationship between weather conditions and 911 incidents

Output:
- datasets/analysis/weather_incident_correlation.parquet
- datasets/analysis/weather_analysis_summary.csv
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
from scipy import stats

def analyze_weather_correlation():
    print("=" * 60)
    print("ANALYZING WEATHER-INCIDENT CORRELATION")
    print("=" * 60)
    
    # 1. Load data
    print("\n[1/5] Loading data...")
    
    df_911 = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\911_calls_enriched.parquet'
    )
    dim_weather = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\dim_weather.parquet'
    )
    
    print(f"   911 calls: {len(df_911):,}")
    print(f"   Weather records: {len(dim_weather):,}")
    
    # 2. Prepare data for joining
    print("\n[2/5] Preparing join keys...")
    
    df_911['response_datetime'] = pd.to_datetime(df_911['response_datetime'])
    df_911['join_hour'] = df_911['response_datetime'].dt.floor('H')
    
    dim_weather['datetime'] = pd.to_datetime(dim_weather['datetime'])
    dim_weather['join_hour'] = dim_weather['datetime'].dt.floor('H')
    
    # 3. Join 911 calls with weather
    print("\n[3/5] Joining 911 calls with weather data...")
    
    df_with_weather = df_911.merge(
        dim_weather[['join_hour', 'temperature_f', 'precipitation_mm', 
                     'weather_category', 'is_raining', 'temp_category', 'humidity_pct']],
        on='join_hour',
        how='inner'
    )
    
    print(f"   Matched records: {len(df_with_weather):,} ({len(df_with_weather)/len(df_911)*100:.1f}%)")
    
    # 4. Analyze correlations
    print("\n[4/5] Calculating correlations and patterns...")
    
    results = {}
    
    # Incidents by weather category
    weather_incidents = df_with_weather.groupby('weather_category').agg({
        'incident_number': 'count',
        'priority_numeric': 'mean',
        'response_time': 'mean'
    }).reset_index()
    weather_incidents.columns = ['weather_category', 'incident_count', 'avg_priority', 'avg_response_time']
    
    # Calculate rate per hour for each weather type
    weather_hours = dim_weather.groupby('weather_category').size().reset_index(name='total_hours')
    weather_incidents = weather_incidents.merge(weather_hours, on='weather_category')
    weather_incidents['incidents_per_hour'] = weather_incidents['incident_count'] / weather_incidents['total_hours']
    weather_incidents = weather_incidents.sort_values('incidents_per_hour', ascending=False)
    
    results['by_weather'] = weather_incidents
    
    # Incidents by temperature category
    temp_incidents = df_with_weather.groupby('temp_category').agg({
        'incident_number': 'count',
        'priority_numeric': 'mean',
        'response_time': 'mean'
    }).reset_index()
    temp_incidents.columns = ['temp_category', 'incident_count', 'avg_priority', 'avg_response_time']
    
    results['by_temperature'] = temp_incidents
    
    # Rain vs No Rain comparison
    rain_comparison = df_with_weather.groupby('is_raining').agg({
        'incident_number': 'count',
        'priority_numeric': 'mean',
        'response_time': 'mean'
    }).reset_index()
    rain_comparison.columns = ['is_raining', 'incident_count', 'avg_priority', 'avg_response_time']
    
    results['rain_comparison'] = rain_comparison
    
    # Correlation coefficients
    correlations = {}
    
    # Temperature vs incident count (hourly aggregation)
    hourly_data = df_with_weather.groupby('join_hour').agg({
        'incident_number': 'count',
        'temperature_f': 'first',
        'precipitation_mm': 'first',
        'humidity_pct': 'first'
    }).reset_index()
    
    if len(hourly_data) > 10:
        corr_temp = stats.pearsonr(hourly_data['temperature_f'], hourly_data['incident_number'])
        correlations['temperature'] = {'r': corr_temp[0], 'p_value': corr_temp[1]}
        
        corr_precip = stats.pearsonr(hourly_data['precipitation_mm'], hourly_data['incident_number'])
        correlations['precipitation'] = {'r': corr_precip[0], 'p_value': corr_precip[1]}
        
        corr_humidity = stats.pearsonr(hourly_data['humidity_pct'], hourly_data['incident_number'])
        correlations['humidity'] = {'r': corr_humidity[0], 'p_value': corr_humidity[1]}
    
    results['correlations'] = correlations
    
    # 5. Save results
    print("\n[5/5] Saving results...")
    
    import os
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis', exist_ok=True)
    
    # Save detailed weather-incident data
    output_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\weather_incident_data.parquet'
    df_with_weather.to_parquet(output_path, index=False)
    print(f"   ✓ Detailed data saved to: {output_path}")
    
    # Save weather analysis summary
    weather_incidents.to_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\incidents_by_weather.csv', 
        index=False
    )
    temp_incidents.to_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\incidents_by_temperature.csv', 
        index=False
    )
    
    # =========================================
    # RESULTS SUMMARY
    # =========================================
    print("\n" + "=" * 60)
    print("WEATHER CORRELATION RESULTS")
    print("=" * 60)
    
    print("\n🌤️ Incidents by Weather Category:")
    print(weather_incidents.to_string(index=False))
    
    print("\n🌡️ Incidents by Temperature:")
    print(temp_incidents.to_string(index=False))
    
    print("\n🌧️ Rain Impact:")
    for _, row in rain_comparison.iterrows():
        status = "Raining" if row['is_raining'] else "Not Raining"
        print(f"   {status}: {row['incident_count']:,} incidents, avg priority {row['avg_priority']:.2f}")
    
    print("\n📊 Correlation Coefficients:")
    for var, vals in correlations.items():
        sig = "***" if vals['p_value'] < 0.001 else "**" if vals['p_value'] < 0.01 else "*" if vals['p_value'] < 0.05 else ""
        print(f"   {var.capitalize()}: r = {vals['r']:.4f}{sig} (p = {vals['p_value']:.4f})")
    
    return results

if __name__ == "__main__":
    results = analyze_weather_correlation()
    print("\n" + "=" * 60)
    print("WEATHER CORRELATION ANALYSIS COMPLETE!")
    print("=" * 60)
