"""
SafeBeat ETL - Weather Data Ingestion
Fetches historical weather data for Austin, TX to enrich incident analysis

This script uses the Open-Meteo API (free, no API key required) to get
historical weather data that can be joined with 911 calls.

Output: datasets/cleaned/dim_weather.parquet
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import requests
from datetime import datetime, timedelta

def fetch_weather_data(start_date: str, end_date: str):
    """
    Fetch historical weather data from Open-Meteo API
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        DataFrame with hourly weather data
    """
    # Austin, TX coordinates
    latitude = 30.2672
    longitude = -97.7431
    
    # Open-Meteo API for historical weather
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m", 
            "precipitation",
            "rain",
            "weather_code",
            "wind_speed_10m",
            "wind_gusts_10m"
        ],
        "timezone": "America/Chicago"  # Austin timezone
    }
    
    print(f"   Fetching weather data from {start_date} to {end_date}...")
    response = requests.get(base_url, params=params)
    
    if response.status_code != 200:
        print(f"   ERROR: API returned status {response.status_code}")
        return None
    
    data = response.json()
    
    # Convert to DataFrame
    hourly = data['hourly']
    df = pd.DataFrame({
        'datetime': pd.to_datetime(hourly['time']),
        'temperature_c': hourly['temperature_2m'],
        'humidity_pct': hourly['relative_humidity_2m'],
        'precipitation_mm': hourly['precipitation'],
        'rain_mm': hourly['rain'],
        'weather_code': hourly['weather_code'],
        'wind_speed_kmh': hourly['wind_speed_10m'],
        'wind_gusts_kmh': hourly['wind_gusts_10m']
    })
    
    return df

def create_dim_weather():
    print("=" * 60)
    print("CREATING DIM_WEATHER TABLE")
    print("=" * 60)
    
    # 1. Determine date range from 911 calls
    print("\n[1/5] Determining date range from 911 calls data...")
    
    try:
        df_911 = pd.read_parquet(
            r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\911_calls_cleaned.parquet',
            columns=['response_datetime']
        )
        min_date = df_911['response_datetime'].min()
        max_date = df_911['response_datetime'].max()
        print(f"   911 calls date range: {min_date.date()} to {max_date.date()}")
    except Exception as e:
        print(f"   Could not read 911 calls: {e}")
        # Default to 2023-2025
        min_date = datetime(2023, 1, 1)
        max_date = datetime(2025, 12, 31)
        print(f"   Using default range: {min_date.date()} to {max_date.date()}")
    
    # 2. Fetch weather data in yearly chunks (API limit)
    print("\n[2/5] Fetching weather data from Open-Meteo API...")
    
    # Open-Meteo historical API has limits, so we fetch in chunks
    all_weather_data = []
    
    # Get years to fetch
    start_year = min_date.year
    end_year = min(max_date.year, 2024)  # API may not have 2025 data yet
    
    for year in range(start_year, end_year + 1):
        year_start = f"{year}-01-01"
        year_end = f"{year}-12-31"
        
        try:
            df_year = fetch_weather_data(year_start, year_end)
            if df_year is not None:
                all_weather_data.append(df_year)
                print(f"   ✓ {year}: {len(df_year):,} hourly records")
        except Exception as e:
            print(f"   ✗ {year}: Failed - {e}")
    
    if not all_weather_data:
        print("   ERROR: No weather data could be fetched!")
        return None
    
    # Combine all years
    df_weather = pd.concat(all_weather_data, ignore_index=True)
    print(f"\n   Total weather records: {len(df_weather):,}")
    
    # 3. Add derived columns
    print("\n[3/5] Adding derived columns...")
    
    # Date components for joining
    df_weather['date'] = df_weather['datetime'].dt.date
    df_weather['hour'] = df_weather['datetime'].dt.hour
    df_weather['day_of_week'] = df_weather['datetime'].dt.day_name()
    df_weather['month'] = df_weather['datetime'].dt.month
    df_weather['year'] = df_weather['datetime'].dt.year
    
    # Temperature in Fahrenheit (for US context)
    df_weather['temperature_f'] = df_weather['temperature_c'] * 9/5 + 32
    
    # Weather condition categories based on WMO codes
    def categorize_weather(code):
        if pd.isna(code):
            return 'Unknown'
        code = int(code)
        if code == 0:
            return 'Clear'
        elif code in [1, 2, 3]:
            return 'Partly Cloudy'
        elif code in [45, 48]:
            return 'Fog'
        elif code in [51, 53, 55, 56, 57]:
            return 'Drizzle'
        elif code in [61, 63, 65, 66, 67]:
            return 'Rain'
        elif code in [71, 73, 75, 77]:
            return 'Snow'
        elif code in [80, 81, 82]:
            return 'Rain Showers'
        elif code in [85, 86]:
            return 'Snow Showers'
        elif code in [95, 96, 99]:
            return 'Thunderstorm'
        else:
            return 'Other'
    
    df_weather['weather_category'] = df_weather['weather_code'].apply(categorize_weather)
    
    # Is it raining?
    df_weather['is_raining'] = df_weather['precipitation_mm'] > 0.1
    
    # Temperature categories
    def temp_category(temp_f):
        if pd.isna(temp_f):
            return 'Unknown'
        if temp_f < 32:
            return 'Freezing'
        elif temp_f < 50:
            return 'Cold'
        elif temp_f < 70:
            return 'Mild'
        elif temp_f < 85:
            return 'Warm'
        else:
            return 'Hot'
    
    df_weather['temp_category'] = df_weather['temperature_f'].apply(temp_category)
    
    print("   ✓ Added: date, hour, day_of_week, month, year")
    print("   ✓ Added: temperature_f, weather_category, is_raining, temp_category")
    
    # 4. Verification
    print("\n" + "=" * 60)
    print("VERIFICATION CHECKS")
    print("=" * 60)
    
    print(f"✓ Total records: {len(df_weather):,}")
    print(f"✓ Date range: {df_weather['datetime'].min()} to {df_weather['datetime'].max()}")
    print(f"✓ Temperature range: {df_weather['temperature_f'].min():.1f}°F to {df_weather['temperature_f'].max():.1f}°F")
    
    print("\n   Weather category distribution:")
    cat_dist = df_weather['weather_category'].value_counts()
    for cat, count in cat_dist.items():
        print(f"      {cat}: {count:,}")
    
    # 5. Save
    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT")
    print("=" * 60)
    sample_cols = ['datetime', 'temperature_f', 'weather_category', 'is_raining', 'precipitation_mm']
    print(df_weather[sample_cols].head(10).to_string(index=False))
    
    output_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\dim_weather.parquet'
    df_weather.to_parquet(output_path, index=False)
    print(f"\n✓ Saved to: {output_path}")
    
    # Also save daily aggregations for easier joining
    print("\n[5/5] Creating daily weather summary...")
    df_daily = df_weather.groupby('date').agg({
        'temperature_f': ['min', 'max', 'mean'],
        'precipitation_mm': 'sum',
        'humidity_pct': 'mean',
        'wind_speed_kmh': 'max',
        'is_raining': 'any'
    }).reset_index()
    
    # Flatten column names
    df_daily.columns = ['date', 'temp_min_f', 'temp_max_f', 'temp_avg_f', 
                        'precipitation_total_mm', 'humidity_avg_pct', 
                        'wind_max_kmh', 'had_rain']
    
    daily_output = r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\dim_weather_daily.parquet'
    df_daily.to_parquet(daily_output, index=False)
    print(f"✓ Daily summary saved to: {daily_output}")
    
    return df_weather

if __name__ == "__main__":
    df = create_dim_weather()
    print("\n" + "=" * 60)
    print("DIM_WEATHER CREATION COMPLETE!")
    print("=" * 60)
