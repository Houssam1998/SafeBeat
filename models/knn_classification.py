"""
SafeBeat ML - KNN Incident Classification
Real-time classification of probable incident type based on context

Algorithm: K-Nearest Neighbors
Use case: Given {location, time, weather}, predict incident type

Output:
- models/knn_classifier.pkl
- models/knn_classification_report.txt
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import pickle
import warnings
warnings.filterwarnings('ignore')

def train_knn_classifier():
    print("=" * 60)
    print("KNN INCIDENT TYPE CLASSIFICATION")
    print("=" * 60)
    
    # 1. Load data
    print("\n[1/7] Loading data...")
    
    try:
        df = pd.read_parquet(
            r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\weather_incident_data.parquet'
        )
        print(f"   Loaded weather-incident data: {len(df):,}")
    except:
        df = pd.read_parquet(
            r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\911_calls_enriched.parquet'
        )
        print(f"   Loaded enriched 911 data: {len(df):,}")
    
    # 2. Prepare features
    print("\n[2/7] Preparing features...")
    
    # Select features that would be available at prediction time
    feature_cols = []
    
    # Location features
    if 'latitude_centroid' in df.columns:
        feature_cols.extend(['latitude_centroid', 'longitude_centroid'])
    
    # Time features
    if 'response_hour' in df.columns:
        df['hour_sin'] = np.sin(2 * np.pi * df['response_hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['response_hour'] / 24)
        feature_cols.extend(['hour_sin', 'hour_cos'])
    
    # Day of week encoding
    if 'response_day_of_week' in df.columns:
        day_map = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}
        df['day_num'] = df['response_day_of_week'].map(day_map)
        df['is_weekend'] = df['day_num'].isin([5, 6]).astype(int)
        feature_cols.extend(['day_num', 'is_weekend'])
    
    # Weather features
    if 'temperature_f' in df.columns:
        feature_cols.extend(['temperature_f', 'humidity_pct', 'precipitation_mm'])
    
    # Priority as feature
    if 'priority_numeric' in df.columns:
        feature_cols.append('priority_numeric')
    
    print(f"   Features: {feature_cols}")
    
    # 3. Prepare target
    print("\n[3/7] Preparing target variable...")
    
    # Use simplified problem categories
    df['incident_category'] = df['final_problem_category'].fillna('Unknown')
    
    # Keep only top categories
    top_categories = df['incident_category'].value_counts().head(10).index.tolist()
    df['incident_category'] = df['incident_category'].apply(
        lambda x: x if x in top_categories else 'Other'
    )
    
    print(f"   Target categories: {len(df['incident_category'].unique())}")
    
    # 4. Clean and prepare data
    print("\n[4/7] Cleaning data...")
    
    # Drop rows with missing values
    df_clean = df[feature_cols + ['incident_category']].dropna()
    print(f"   Clean samples: {len(df_clean):,}")
    
    # Sample if too large
    if len(df_clean) > 100000:
        print("   Sampling to 100,000 for training...")
        df_clean = df_clean.sample(n=100000, random_state=42)
    
    X = df_clean[feature_cols].values
    y = df_clean['incident_category'].values
    
    # Encode labels
    le = LabelEncoder()
    y_encoded = le.fit_transform(y)
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 5. Train/test split
    print("\n[5/7] Splitting data...")
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
    )
    print(f"   Training set: {len(X_train):,}")
    print(f"   Test set: {len(X_test):,}")
    
    # 6. Train KNN with optimal K
    print("\n[6/7] Training KNN classifier...")
    
    # Find optimal K
    k_range = range(3, 15, 2)
    accuracies = []
    
    for k in k_range:
        knn = KNeighborsClassifier(n_neighbors=k, n_jobs=-1)
        knn.fit(X_train, y_train)
        acc = knn.score(X_test, y_test)
        accuracies.append(acc)
        print(f"   K={k}: Accuracy={acc:.4f}")
    
    best_k = k_range[np.argmax(accuracies)]
    print(f"\n   ✓ Best K: {best_k} (Accuracy: {max(accuracies):.4f})")
    
    # Train final model
    knn_final = KNeighborsClassifier(n_neighbors=best_k, n_jobs=-1)
    knn_final.fit(X_train, y_train)
    
    # Predict
    y_pred = knn_final.predict(X_test)
    
    # 7. Evaluate and save
    print("\n[7/7] Evaluating and saving model...")
    
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, target_names=le.classes_, zero_division=0)
    
    import os
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\models', exist_ok=True)
    
    # Save model
    with open(r'd:\uemf\s9\Data mining\SafeBeat\models\knn_classifier.pkl', 'wb') as f:
        pickle.dump({
            'model': knn_final,
            'scaler': scaler,
            'label_encoder': le,
            'features': feature_cols,
            'best_k': best_k
        }, f)
    
    # Save report
    with open(r'd:\uemf\s9\Data mining\SafeBeat\models\knn_classification_report.txt', 'w') as f:
        f.write("KNN INCIDENT CLASSIFICATION REPORT\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Best K: {best_k}\n")
        f.write(f"Overall Accuracy: {accuracy:.4f}\n\n")
        f.write("Classification Report:\n")
        f.write(report)
    
    print("   ✓ Model saved")
    print("   ✓ Report saved")
    
    # =========================================
    # RESULTS
    # =========================================
    print("\n" + "=" * 60)
    print("KNN CLASSIFICATION RESULTS")
    print("=" * 60)
    
    print(f"\n📊 OVERALL ACCURACY: {accuracy:.2%}")
    print(f"\n📋 CLASSIFICATION REPORT:")
    print(report)
    
    # Top predicted classes
    print("\n🎯 INCIDENT CATEGORY DISTRIBUTION (Test Set):")
    unique, counts = np.unique(y_test, return_counts=True)
    for idx, count in zip(unique, counts):
        print(f"   {le.classes_[idx]}: {count:,}")
    
    return knn_final, accuracy, le.classes_

if __name__ == "__main__":
    model, accuracy, classes = train_knn_classifier()
    print("\n" + "=" * 60)
    print("KNN CLASSIFICATION COMPLETE!")
    print("=" * 60)
