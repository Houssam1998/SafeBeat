"""
SafeBeat ML - K-Means Clustering for Zone Segmentation
Segments festival areas into High-Risk Zones based on incident density

Algorithm: K-Means Clustering
Use case: Identify geographic clusters of incident hotspots

Output: 
- models/kmeans_zone_clusters.parquet
- models/kmeans_model.pkl
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import pickle
import warnings
warnings.filterwarnings('ignore')

def cluster_risk_zones():
    print("=" * 60)
    print("K-MEANS CLUSTERING - RISK ZONE SEGMENTATION")
    print("=" * 60)
    
    # 1. Load data
    print("\n[1/6] Loading incident data...")
    
    df = pd.read_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\911_calls_enriched.parquet'
    )
    
    # Filter to incidents with coordinates
    df = df[df['latitude_centroid'].notna()].copy()
    print(f"   Incidents with coordinates: {len(df):,}")
    
    # 2. Prepare features for clustering
    print("\n[2/6] Preparing features...")
    
    # Aggregate by geo location
    geo_features = df.groupby(['latitude_centroid', 'longitude_centroid']).agg({
        'incident_number': 'count',
        'priority_numeric': 'mean',
        'response_time': 'mean',
        'response_hour': 'mean'
    }).reset_index()
    
    geo_features.columns = [
        'latitude', 'longitude', 'incident_count',
        'avg_priority', 'avg_response_time', 'avg_hour'
    ]
    
    print(f"   Unique locations: {len(geo_features):,}")
    
    # 3. Feature engineering
    print("\n[3/6] Feature engineering...")
    
    # Normalize incident count (log scale for better clustering)
    geo_features['log_incidents'] = np.log1p(geo_features['incident_count'])
    
    # Risk score per location
    geo_features['location_risk'] = (
        geo_features['log_incidents'] * 0.5 +
        (4 - geo_features['avg_priority']) * 0.3 +  # Lower priority = higher risk
        (geo_features['avg_response_time'] / 600) * 0.2  # Normalized response time
    )
    
    # Prepare clustering features
    features = ['latitude', 'longitude', 'log_incidents', 'avg_priority', 'location_risk']
    X = geo_features[features].values
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    print(f"   Feature matrix shape: {X_scaled.shape}")
    
    # 4. Find optimal number of clusters
    print("\n[4/6] Finding optimal K (Elbow method)...")
    
    inertias = []
    silhouettes = []
    K_range = range(3, 10)
    
    for k in K_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(X_scaled)
        inertias.append(kmeans.inertia_)
        silhouettes.append(silhouette_score(X_scaled, kmeans.labels_))
        print(f"   K={k}: Inertia={kmeans.inertia_:.0f}, Silhouette={silhouettes[-1]:.3f}")
    
    # Choose K with best silhouette
    best_k = K_range[np.argmax(silhouettes)]
    print(f"\n   ✓ Optimal K (best silhouette): {best_k}")
    
    # 5. Train final model
    print(f"\n[5/6] Training K-Means with K={best_k}...")
    
    kmeans_final = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    geo_features['cluster'] = kmeans_final.fit_predict(X_scaled)
    
    # Analyze clusters
    cluster_stats = geo_features.groupby('cluster').agg({
        'latitude': 'mean',
        'longitude': 'mean',
        'incident_count': ['sum', 'mean'],
        'avg_priority': 'mean',
        'location_risk': 'mean'
    }).round(2)
    
    cluster_stats.columns = ['center_lat', 'center_lon', 'total_incidents', 
                             'avg_incidents', 'avg_priority', 'avg_risk']
    cluster_stats = cluster_stats.reset_index()
    
    # Assign risk labels
    risk_thresholds = cluster_stats['avg_risk'].quantile([0.33, 0.66]).values
    
    def label_risk(risk):
        if risk >= risk_thresholds[1]:
            return 'HIGH_RISK'
        elif risk >= risk_thresholds[0]:
            return 'MEDIUM_RISK'
        else:
            return 'LOW_RISK'
    
    cluster_stats['risk_zone'] = cluster_stats['avg_risk'].apply(label_risk)
    geo_features['risk_zone'] = geo_features['cluster'].map(
        cluster_stats.set_index('cluster')['risk_zone']
    )
    
    # 6. Save results
    print("\n[6/6] Saving results...")
    
    import os
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\models', exist_ok=True)
    
    # Save clustered data
    geo_features.to_parquet(
        r'd:\uemf\s9\Data mining\SafeBeat\models\kmeans_zone_clusters.parquet',
        index=False
    )
    
    # Save cluster statistics
    cluster_stats.to_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\models\kmeans_cluster_stats.csv',
        index=False
    )
    
    # Save model
    with open(r'd:\uemf\s9\Data mining\SafeBeat\models\kmeans_model.pkl', 'wb') as f:
        pickle.dump({'model': kmeans_final, 'scaler': scaler, 'features': features}, f)
    
    print("   ✓ Clustered data saved")
    print("   ✓ Cluster statistics saved")
    print("   ✓ Model saved")
    
    # =========================================
    # RESULTS
    # =========================================
    print("\n" + "=" * 60)
    print("CLUSTERING RESULTS")
    print("=" * 60)
    
    print("\n📊 CLUSTER STATISTICS:")
    print(cluster_stats.to_string(index=False))
    
    print("\n🗺️ ZONE DISTRIBUTION:")
    zone_dist = geo_features['risk_zone'].value_counts()
    for zone, count in zone_dist.items():
        print(f"   {zone}: {count:,} locations")
    
    print("\n🔴 HIGH-RISK ZONE DETAILS:")
    high_risk = cluster_stats[cluster_stats['risk_zone'] == 'HIGH_RISK']
    if not high_risk.empty:
        for _, row in high_risk.iterrows():
            print(f"   Cluster {row['cluster']}:")
            print(f"     Center: ({row['center_lat']:.4f}, {row['center_lon']:.4f})")
            print(f"     Total incidents: {row['total_incidents']:,.0f}")
            print(f"     Average priority: {row['avg_priority']:.2f}")
    
    return geo_features, cluster_stats, kmeans_final

if __name__ == "__main__":
    geo_features, cluster_stats, model = cluster_risk_zones()
    print("\n" + "=" * 60)
    print("K-MEANS CLUSTERING COMPLETE!")
    print("=" * 60)
