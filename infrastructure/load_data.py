"""
SafeBeat - Data Loader Script
Uploads local data to MinIO and PostgreSQL for the production pipeline
Run this once to populate the infrastructure with initial data
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import os
from datetime import datetime

# ============================================
# Configuration
# ============================================
CONFIG = {
    'minio': {
        'endpoint': 'localhost:9000',
        'access_key': 'safebeat_admin',
        'secret_key': 'safebeat_secret_2024',
    },
    'postgres': {
        'host': 'localhost',
        'port': 5432,
        'database': 'safebeat',
        'user': 'safebeat_user',
        'password': 'safebeat_db_2024'
    },
    'local_paths': {
        'cleaned': r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned',
        'analysis': r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis',
        'enriched': r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched',
        'models': r'd:\uemf\s9\Data mining\SafeBeat\models'
    }
}


def upload_to_minio():
    """Upload local files to MinIO buckets"""
    print("\n" + "=" * 50)
    print("📦 Uploading to MinIO")
    print("=" * 50)
    
    try:
        from minio import Minio
        
        client = Minio(
            CONFIG['minio']['endpoint'],
            access_key=CONFIG['minio']['access_key'],
            secret_key=CONFIG['minio']['secret_key'],
            secure=False
        )
        
        # Ensure buckets exist
        for bucket in ['raw-data', 'cleaned-data', 'models', 'reports']:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"Created bucket: {bucket}")
        
        uploaded_count = 0
        
        # Upload cleaned CSV files
        cleaned_path = CONFIG['local_paths']['cleaned']
        for filename in os.listdir(cleaned_path):
            if filename.endswith('.csv'):
                filepath = os.path.join(cleaned_path, filename)
                object_name = f"cleaned/{filename}"
                client.fput_object('cleaned-data', object_name, filepath)
                print(f"✓ Uploaded: {filename}")
                uploaded_count += 1
        
        # Upload analysis files
        analysis_path = CONFIG['local_paths']['analysis']
        for filename in os.listdir(analysis_path):
            if filename.endswith('.csv'):
                filepath = os.path.join(analysis_path, filename)
                object_name = f"analysis/{filename}"
                client.fput_object('cleaned-data', object_name, filepath)
                print(f"✓ Uploaded: {filename}")
                uploaded_count += 1
        
        # Upload model files (only small ones)
        models_path = CONFIG['local_paths']['models']
        for filename in os.listdir(models_path):
            if filename.endswith(('.csv', '.txt')):
                filepath = os.path.join(models_path, filename)
                object_name = f"models/{filename}"
                client.fput_object('models', object_name, filepath)
                print(f"✓ Uploaded: {filename}")
                uploaded_count += 1
        
        print(f"\n✅ Total uploaded: {uploaded_count} files")
        return uploaded_count
        
    except Exception as e:
        print(f"❌ MinIO Error: {e}")
        return 0


def load_to_postgres():
    """Load sample data to PostgreSQL tables"""
    print("\n" + "=" * 50)
    print("🐘 Loading to PostgreSQL")
    print("=" * 50)
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=CONFIG['postgres']['host'],
            port=CONFIG['postgres']['port'],
            database=CONFIG['postgres']['database'],
            user=CONFIG['postgres']['user'],
            password=CONFIG['postgres']['password']
        )
        cursor = conn.cursor()
        
        loaded_count = 0
        
        # Load dim_geo from CSV
        geo_path = os.path.join(CONFIG['local_paths']['cleaned'], 'dim_geo_lookup.csv')
        if os.path.exists(geo_path):
            df_geo = pd.read_csv(geo_path)
            print(f"Loading {len(df_geo)} geo records...")
            
            for _, row in df_geo.head(100).iterrows():  # Load first 100 for demo
                try:
                    cursor.execute("""
                        INSERT INTO dim_geo (geo_id, latitude_centroid, longitude_centroid, geom)
                        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                        ON CONFLICT (geo_id) DO NOTHING
                    """, (
                        row['geo_id'],
                        row['latitude_centroid'],
                        row['longitude_centroid'],
                        row['longitude_centroid'],
                        row['latitude_centroid']
                    ))
                except Exception as e:
                    continue
            
            conn.commit()
            cursor.execute("SELECT COUNT(*) FROM dim_geo")
            count = cursor.fetchone()[0]
            print(f"✓ dim_geo: {count} records")
            loaded_count += count
        
        # Load risk scores
        risk_path = os.path.join(CONFIG['local_paths']['analysis'], 'festival_risk_scores.csv')
        if os.path.exists(risk_path):
            df_risk = pd.read_csv(risk_path)
            print(f"Loading {len(df_risk)} risk scores...")
            
            cursor.execute("TRUNCATE TABLE analysis_risk_scores CASCADE")
            
            for _, row in df_risk.iterrows():
                try:
                    cursor.execute("""
                        INSERT INTO analysis_risk_scores 
                        (event_id, event_name, incident_count, avg_priority, risk_score, risk_category, has_alcohol)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO UPDATE SET
                            risk_score = EXCLUDED.risk_score,
                            risk_category = EXCLUDED.risk_category
                    """, (
                        int(row.get('event_id', 0)) if pd.notna(row.get('event_id')) else 0,
                        str(row.get('event_name', 'Unknown'))[:500],
                        int(row.get('incident_count', 0)),
                        float(row.get('avg_priority', 0)) if pd.notna(row.get('avg_priority')) else 0,
                        float(row.get('risk_score', 0)) if pd.notna(row.get('risk_score')) else 0,
                        row.get('risk_category', 'LOW'),
                        bool(row.get('has_alcohol', False))
                    ))
                except Exception as e:
                    continue
            
            conn.commit()
            cursor.execute("SELECT COUNT(*) FROM analysis_risk_scores")
            count = cursor.fetchone()[0]
            print(f"✓ analysis_risk_scores: {count} records")
            loaded_count += count
        
        # Load cluster zones
        clusters_path = os.path.join(CONFIG['local_paths']['models'], 'kmeans_cluster_stats.csv')
        if os.path.exists(clusters_path):
            df_clusters = pd.read_csv(clusters_path)
            print(f"Loading {len(df_clusters)} cluster zones...")
            
            for _, row in df_clusters.iterrows():
                try:
                    cursor.execute("""
                        INSERT INTO analysis_zone_clusters 
                        (latitude, longitude, incident_count, risk_zone, geom)
                        VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                    """, (
                        row.get('center_lat', 30.27),
                        row.get('center_lon', -97.74),
                        int(row.get('total_incidents', 0)),
                        row.get('risk_zone', 'LOW_RISK'),
                        row.get('center_lon', -97.74),
                        row.get('center_lat', 30.27)
                    ))
                except Exception as e:
                    continue
            
            conn.commit()
            cursor.execute("SELECT COUNT(*) FROM analysis_zone_clusters")
            count = cursor.fetchone()[0]
            print(f"✓ analysis_zone_clusters: {count} records")
            loaded_count += count
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Total loaded to PostgreSQL: {loaded_count} records")
        return loaded_count
        
    except Exception as e:
        print(f"❌ PostgreSQL Error: {e}")
        import traceback
        traceback.print_exc()
        return 0


def verify_data():
    """Verify data was loaded correctly"""
    print("\n" + "=" * 50)
    print("🔍 Verification")
    print("=" * 50)
    
    # Verify MinIO
    try:
        from minio import Minio
        
        client = Minio(
            CONFIG['minio']['endpoint'],
            access_key=CONFIG['minio']['access_key'],
            secret_key=CONFIG['minio']['secret_key'],
            secure=False
        )
        
        for bucket in ['raw-data', 'cleaned-data', 'models', 'reports']:
            objects = list(client.list_objects(bucket, recursive=True))
            print(f"MinIO {bucket}: {len(objects)} objects")
            
    except Exception as e:
        print(f"MinIO verification error: {e}")
    
    # Verify PostgreSQL
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=CONFIG['postgres']['host'],
            port=CONFIG['postgres']['port'],
            database=CONFIG['postgres']['database'],
            user=CONFIG['postgres']['user'],
            password=CONFIG['postgres']['password']
        )
        cursor = conn.cursor()
        
        tables = ['dim_geo', 'dim_event', 'fact_911_calls', 'analysis_risk_scores', 'analysis_zone_clusters']
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"PostgreSQL {table}: {count} records")
            except:
                print(f"PostgreSQL {table}: table empty or not exists")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"PostgreSQL verification error: {e}")


if __name__ == "__main__":
    print("🎵 SafeBeat Data Loader")
    print("=" * 50)
    print(f"Started at: {datetime.now()}")
    
    # Upload to MinIO
    minio_count = upload_to_minio()
    
    # Load to PostgreSQL
    postgres_count = load_to_postgres()
    
    # Verify
    verify_data()
    
    print("\n" + "=" * 50)
    print("✅ DATA LOADING COMPLETE")
    print("=" * 50)
    print(f"MinIO files: {minio_count}")
    print(f"PostgreSQL records: {postgres_count}")
    print("\nYou can now run the Airflow DAG!")
