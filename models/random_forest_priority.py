"""
SafeBeat ML - Random Forest for Priority Prediction
Predicts incident priority level for resource allocation

Algorithm: Random Forest Classifier
Use case: Predict priority based on incident context for proactive resource deployment

Output:
- models/rf_priority_model.pkl
- models/rf_priority_report.txt
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
import pickle
import warnings
warnings.filterwarnings('ignore')

def train_priority_predictor():
    print("=" * 60)
    print("RANDOM FOREST - PRIORITY PREDICTION")
    print("=" * 60)
    
    # 1. Load data
    print("\n[1/6] Loading data...")
    
    try:
        df = pd.read_parquet(
            r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\weather_incident_data.parquet'
        )
        has_weather = True
    except:
        df = pd.read_parquet(
            r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\911_calls_enriched.parquet'
        )
        has_weather = False
    
    print(f"   Loaded: {len(df):,} records")
    
    # 2. Prepare features
    print("\n[2/6] Preparing features...")
    
    feature_cols = []
    
    # Location
    if 'latitude_centroid' in df.columns:
        feature_cols.extend(['latitude_centroid', 'longitude_centroid'])
    
    # Time features
    if 'response_hour' in df.columns:
        df['hour_sin'] = np.sin(2 * np.pi * df['response_hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['response_hour'] / 24)
        feature_cols.extend(['hour_sin', 'hour_cos', 'response_hour'])
    
    # Day of week
    if 'response_day_of_week' in df.columns:
        day_map = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}
        df['day_num'] = df['response_day_of_week'].map(day_map)
        df['is_weekend'] = (df['day_num'] >= 5).astype(int)
        feature_cols.extend(['day_num', 'is_weekend'])
    
    # Weather
    if has_weather and 'temperature_f' in df.columns:
        weather_cols = ['temperature_f', 'humidity_pct', 'precipitation_mm']
        weather_cols = [c for c in weather_cols if c in df.columns]
        feature_cols.extend(weather_cols)
    
    # Incident category encoding
    if 'initial_problem_category' in df.columns:
        le_category = LabelEncoder()
        df['category_encoded'] = le_category.fit_transform(df['initial_problem_category'].fillna('Unknown'))
        feature_cols.append('category_encoded')
    
    print(f"   Features: {feature_cols}")
    
    # 3. Prepare target
    print("\n[3/6] Preparing target...")
    
    df = df[df['priority_level'].notna()]
    
    # Map priority to numeric classes
    priority_map = {
        'Priority 0': 0,
        'Priority 1': 1,
        'Priority 2': 2,
        'Priority 3': 3
    }
    df['priority_class'] = df['priority_level'].map(priority_map)
    df = df[df['priority_class'].notna()]
    
    print(f"   Priority distribution:")
    for p, count in df['priority_level'].value_counts().items():
        print(f"      {p}: {count:,}")
    
    # 4. Prepare data
    print("\n[4/6] Preparing training data...")
    
    df_clean = df[feature_cols + ['priority_class']].dropna()
    print(f"   Clean samples: {len(df_clean):,}")
    
    if len(df_clean) > 100000:
        df_clean = df_clean.sample(n=100000, random_state=42)
        print("   Sampled to 100,000")
    
    X = df_clean[feature_cols].values
    y = df_clean['priority_class'].values.astype(int)
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 5. Train model
    print("\n[5/6] Training Random Forest...")
    
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Random Forest with tuned parameters
    rf = RandomForestClassifier(
        n_estimators=100,
        max_depth=15,
        min_samples_split=10,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1
    )
    
    rf.fit(X_train, y_train)
    
    # Cross-validation
    cv_scores = cross_val_score(rf, X_train, y_train, cv=5)
    print(f"   Cross-validation: {cv_scores.mean():.4f} (+/- {cv_scores.std()*2:.4f})")
    
    # Predictions
    y_pred = rf.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"   Test accuracy: {accuracy:.4f}")
    
    # 6. Save model
    print("\n[6/6] Saving model...")
    
    import os
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\models', exist_ok=True)
    
    # Classification report
    class_names = ['Priority_0', 'Priority_1', 'Priority_2', 'Priority_3']
    report = classification_report(y_test, y_pred, target_names=class_names, zero_division=0)
    
    with open(r'd:\uemf\s9\Data mining\SafeBeat\models\rf_priority_model.pkl', 'wb') as f:
        pickle.dump({
            'model': rf,
            'scaler': scaler,
            'features': feature_cols,
            'accuracy': accuracy
        }, f)
    
    with open(r'd:\uemf\s9\Data mining\SafeBeat\models\rf_priority_report.txt', 'w') as f:
        f.write("RANDOM FOREST PRIORITY PREDICTION REPORT\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Accuracy: {accuracy:.4f}\n")
        f.write(f"CV Score: {cv_scores.mean():.4f}\n\n")
        f.write("Classification Report:\n")
        f.write(report)
    
    # Feature importance
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': rf.feature_importances_
    }).sort_values('importance', ascending=False)
    
    importance.to_csv(
        r'd:\uemf\s9\Data mining\SafeBeat\models\rf_feature_importance.csv',
        index=False
    )
    
    print("   ✓ Model saved")
    print("   ✓ Report saved")
    
    # =========================================
    # RESULTS
    # =========================================
    print("\n" + "=" * 60)
    print("RANDOM FOREST RESULTS")
    print("=" * 60)
    
    print(f"\n📊 ACCURACY: {accuracy:.2%}")
    print(f"\n📋 CLASSIFICATION REPORT:")
    print(report)
    
    print("\n🔑 TOP 5 IMPORTANT FEATURES:")
    for _, row in importance.head(5).iterrows():
        print(f"   {row['feature']}: {row['importance']:.4f}")
    
    return rf, accuracy

if __name__ == "__main__":
    model, accuracy = train_priority_predictor()
    print("\n" + "=" * 60)
    print("RANDOM FOREST TRAINING COMPLETE!")
    print("=" * 60)
