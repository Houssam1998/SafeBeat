"""
SafeBeat ML - Master Script
Runs all ML models and generates comprehensive results

This script orchestrates:
1. Association Rules Mining
2. K-Means Clustering
3. KNN Classification
4. Time Series Forecasting
5. Random Forest Priority Prediction
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import os
import pandas as pd
from datetime import datetime

def run_all_models():
    print("=" * 70)
    print("🎵 SAFEBEAT - DATA MINING & AI PIPELINE")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = {}
    
    # Create output directory
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\models', exist_ok=True)
    
    # 1. Association Rules
    print("\n" + "=" * 70)
    print("1️⃣ ASSOCIATION RULES MINING")
    print("=" * 70)
    try:
        from association_rules import mine_association_rules
        rules = mine_association_rules()
        results['association_rules'] = {'status': 'SUCCESS', 'rules_count': len(rules) if rules is not None else 0}
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results['association_rules'] = {'status': 'FAILED', 'error': str(e)}
    
    # 2. K-Means Clustering
    print("\n" + "=" * 70)
    print("2️⃣ K-MEANS CLUSTERING")
    print("=" * 70)
    try:
        from kmeans_clustering import cluster_risk_zones
        geo_features, cluster_stats, _ = cluster_risk_zones()
        results['kmeans'] = {
            'status': 'SUCCESS', 
            'clusters': len(cluster_stats),
            'locations': len(geo_features)
        }
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results['kmeans'] = {'status': 'FAILED', 'error': str(e)}
    
    # 3. KNN Classification
    print("\n" + "=" * 70)
    print("3️⃣ KNN INCIDENT CLASSIFICATION")
    print("=" * 70)
    try:
        from knn_classification import train_knn_classifier
        _, accuracy, classes = train_knn_classifier()
        results['knn'] = {
            'status': 'SUCCESS',
            'accuracy': accuracy,
            'classes': len(classes)
        }
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results['knn'] = {'status': 'FAILED', 'error': str(e)}
    
    # 4. Time Series Forecasting
    print("\n" + "=" * 70)
    print("4️⃣ TIME SERIES FORECASTING")
    print("=" * 70)
    try:
        from timeseries_forecast import forecast_workload
        _, forecast, metrics = forecast_workload()
        results['timeseries'] = {
            'status': 'SUCCESS',
            'r2': metrics['R2'],
            'mae': metrics['MAE']
        }
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results['timeseries'] = {'status': 'FAILED', 'error': str(e)}
    
    # 5. Random Forest Priority
    print("\n" + "=" * 70)
    print("5️⃣ RANDOM FOREST PRIORITY PREDICTION")
    print("=" * 70)
    try:
        from random_forest_priority import train_priority_predictor
        _, accuracy = train_priority_predictor()
        results['random_forest'] = {
            'status': 'SUCCESS',
            'accuracy': accuracy
        }
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results['random_forest'] = {'status': 'FAILED', 'error': str(e)}
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 PIPELINE SUMMARY")
    print("=" * 70)
    
    for model, result in results.items():
        status = "✅" if result['status'] == 'SUCCESS' else "❌"
        print(f"\n{status} {model.upper()}:")
        for key, value in result.items():
            if key != 'status':
                if isinstance(value, float):
                    print(f"      {key}: {value:.4f}")
                else:
                    print(f"      {key}: {value}")
    
    # Save summary
    summary_df = pd.DataFrame([
        {'model': k, **v} for k, v in results.items()
    ])
    summary_df.to_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\models\pipeline_summary.csv',
        index=False
    )
    
    print(f"\n\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    return results

if __name__ == "__main__":
    results = run_all_models()
