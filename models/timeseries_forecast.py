"""
SafeBeat ML - Time Series Forecasting
Predicts 911 call workload based on historical patterns, weather, and events

Algorithms: ARIMA, Prophet-like decomposition
Use case: ŷ(t+h) = f(Y_t, Weather_t, Events_t)

Output:
- models/timeseries_forecast.parquet
- models/timeseries_model.pkl
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
import pickle
import warnings
warnings.filterwarnings('ignore')

def create_lag_features(df, target_col, lags):
    """Create lagged features for time series"""
    for lag in lags:
        df[f'{target_col}_lag_{lag}'] = df[target_col].shift(lag)
    return df

def create_rolling_features(df, target_col, windows):
    """Create rolling statistics"""
    for window in windows:
        df[f'{target_col}_rolling_mean_{window}'] = df[target_col].rolling(window=window).mean()
        df[f'{target_col}_rolling_std_{window}'] = df[target_col].rolling(window=window).std()
    return df

def forecast_workload():
    print("=" * 60)
    print("TIME SERIES FORECASTING - WORKLOAD PREDICTION")
    print("=" * 60)
    
    # 1. Load and prepare data
    print("\n[1/7] Loading data...")
    
    df_911 = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\911_calls_enriched.parquet'
    )
    df_911['response_datetime'] = pd.to_datetime(df_911['response_datetime'])
    
    # Load weather data
    try:
        df_weather = pd.read_parquet(
            r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned\dim_weather_daily.parquet'
        )
        df_weather['date'] = pd.to_datetime(df_weather['date'])
        has_weather = True
    except:
        has_weather = False
        print("   ⚠ Weather data not available, proceeding without it")
    
    print(f"   911 calls: {len(df_911):,}")
    
    # 2. Aggregate to daily level
    print("\n[2/7] Aggregating to daily level...")
    
    df_911['date'] = df_911['response_datetime'].dt.date
    
    daily = df_911.groupby('date').agg({
        'incident_number': 'count',
        'priority_numeric': 'mean',
        'response_time': 'mean'
    }).reset_index()
    
    daily.columns = ['date', 'call_count', 'avg_priority', 'avg_response_time']
    daily['date'] = pd.to_datetime(daily['date'])
    daily = daily.sort_values('date').reset_index(drop=True)
    
    print(f"   Daily records: {len(daily):,}")
    print(f"   Date range: {daily['date'].min()} to {daily['date'].max()}")
    
    # 3. Feature engineering
    print("\n[3/7] Engineering features...")
    
    # Time features
    daily['day_of_week'] = daily['date'].dt.dayofweek
    daily['month'] = daily['date'].dt.month
    daily['day_of_month'] = daily['date'].dt.day
    daily['is_weekend'] = (daily['day_of_week'] >= 5).astype(int)
    
    # Cyclical encoding
    daily['dow_sin'] = np.sin(2 * np.pi * daily['day_of_week'] / 7)
    daily['dow_cos'] = np.cos(2 * np.pi * daily['day_of_week'] / 7)
    daily['month_sin'] = np.sin(2 * np.pi * daily['month'] / 12)
    daily['month_cos'] = np.cos(2 * np.pi * daily['month'] / 12)
    
    # Lag features
    daily = create_lag_features(daily, 'call_count', [1, 7, 14, 28])
    
    # Rolling features
    daily = create_rolling_features(daily, 'call_count', [7, 14, 30])
    
    # Merge weather if available
    if has_weather:
        daily = daily.merge(df_weather, on='date', how='left')
        print("   ✓ Weather features merged")
    
    # 4. Prepare training data
    print("\n[4/7] Preparing training data...")
    
    # Drop rows with NaN (from lag features)
    daily_clean = daily.dropna()
    print(f"   Clean records after lag features: {len(daily_clean):,}")
    
    # Define features
    feature_cols = [
        'day_of_week', 'month', 'is_weekend',
        'dow_sin', 'dow_cos', 'month_sin', 'month_cos',
        'call_count_lag_1', 'call_count_lag_7', 'call_count_lag_14',
        'call_count_rolling_mean_7', 'call_count_rolling_mean_14'
    ]
    
    if has_weather:
        weather_features = ['temp_avg_f', 'precipitation_total_mm', 'humidity_avg_pct']
        weather_features = [f for f in weather_features if f in daily_clean.columns]
        feature_cols.extend(weather_features)
    
    # Ensure all features exist
    feature_cols = [f for f in feature_cols if f in daily_clean.columns]
    print(f"   Features: {feature_cols}")
    
    X = daily_clean[feature_cols].values
    y = daily_clean['call_count'].values
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 5. Train/test split (time-based)
    print("\n[5/7] Time-based train/test split...")
    
    split_idx = int(len(X_scaled) * 0.8)
    X_train, X_test = X_scaled[:split_idx], X_scaled[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    print(f"   Training: {len(X_train):,}")
    print(f"   Testing: {len(X_test):,}")
    
    # 6. Train models
    print("\n[6/7] Training forecasting models...")
    
    # Model 1: Ridge Regression
    ridge = Ridge(alpha=1.0)
    ridge.fit(X_train, y_train)
    y_pred_ridge = ridge.predict(X_test)
    
    ridge_mae = mean_absolute_error(y_test, y_pred_ridge)
    ridge_rmse = np.sqrt(mean_squared_error(y_test, y_pred_ridge))
    ridge_r2 = r2_score(y_test, y_pred_ridge)
    print(f"   Ridge: MAE={ridge_mae:.2f}, RMSE={ridge_rmse:.2f}, R²={ridge_r2:.4f}")
    
    # Model 2: Gradient Boosting
    gb = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
    gb.fit(X_train, y_train)
    y_pred_gb = gb.predict(X_test)
    
    gb_mae = mean_absolute_error(y_test, y_pred_gb)
    gb_rmse = np.sqrt(mean_squared_error(y_test, y_pred_gb))
    gb_r2 = r2_score(y_test, y_pred_gb)
    print(f"   Gradient Boosting: MAE={gb_mae:.2f}, RMSE={gb_rmse:.2f}, R²={gb_r2:.4f}")
    
    # Choose best model
    if gb_r2 > ridge_r2:
        best_model = gb
        best_name = "Gradient Boosting"
        best_metrics = {'MAE': gb_mae, 'RMSE': gb_rmse, 'R2': gb_r2}
        y_pred = y_pred_gb
    else:
        best_model = ridge
        best_name = "Ridge"
        best_metrics = {'MAE': ridge_mae, 'RMSE': ridge_rmse, 'R2': ridge_r2}
        y_pred = y_pred_ridge
    
    print(f"\n   ✓ Best model: {best_name}")
    
    # 7. Save results
    print("\n[7/7] Saving results...")
    
    import os
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\models', exist_ok=True)
    
    # Save forecast
    forecast_df = pd.DataFrame({
        'date': daily_clean['date'].iloc[split_idx:].values,
        'actual': y_test,
        'predicted': y_pred
    })
    forecast_df.to_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\models\timeseries_forecast.parquet',
        index=False
    )
    
    # Save model
    with open(r'd:\uemf\s9\Data mining\SafeBeat\models\timeseries_model.pkl', 'wb') as f:
        pickle.dump({
            'model': best_model,
            'model_name': best_name,
            'scaler': scaler,
            'features': feature_cols,
            'metrics': best_metrics
        }, f)
    
    # Feature importance (for GB)
    if best_name == "Gradient Boosting":
        importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': best_model.feature_importances_
        }).sort_values('importance', ascending=False)
        importance.to_csv(
            r'd:\uemf\s9\Data mining\SafeBeat\models\timeseries_feature_importance.csv',
            index=False
        )
    
    print("   ✓ Forecast saved")
    print("   ✓ Model saved")
    
    # =========================================
    # RESULTS
    # =========================================
    print("\n" + "=" * 60)
    print("TIME SERIES FORECASTING RESULTS")
    print("=" * 60)
    
    print(f"\n📈 BEST MODEL: {best_name}")
    print(f"   MAE: {best_metrics['MAE']:.2f} calls/day")
    print(f"   RMSE: {best_metrics['RMSE']:.2f} calls/day")
    print(f"   R²: {best_metrics['R2']:.4f}")
    
    print(f"\n📊 FORECAST ACCURACY:")
    print(f"   Average daily calls (actual): {y_test.mean():.1f}")
    print(f"   Average daily calls (predicted): {y_pred.mean():.1f}")
    print(f"   MAPE: {np.mean(np.abs(y_test - y_pred) / y_test) * 100:.1f}%")
    
    if best_name == "Gradient Boosting":
        print("\n🔑 TOP 5 IMPORTANT FEATURES:")
        for _, row in importance.head(5).iterrows():
            print(f"   {row['feature']}: {row['importance']:.4f}")
    
    return best_model, forecast_df, best_metrics

if __name__ == "__main__":
    model, forecast, metrics = forecast_workload()
    print("\n" + "=" * 60)
    print("TIME SERIES FORECASTING COMPLETE!")
    print("=" * 60)
