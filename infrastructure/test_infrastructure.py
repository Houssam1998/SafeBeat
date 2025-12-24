"""
SafeBeat - Infrastructure Test Script
Tests connection to MinIO, PostgreSQL, and loads sample data (without modifying local files)
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
from datetime import datetime

# ============================================
# PostgreSQL Test
# ============================================
def test_postgres():
    """Test PostgreSQL + PostGIS connection"""
    print("\n" + "=" * 50)
    print("🐘 Testing PostgreSQL + PostGIS")
    print("=" * 50)
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="safebeat",
            user="safebeat_user",
            password="safebeat_db_2024"
        )
        
        cursor = conn.cursor()
        
        # Test PostGIS
        cursor.execute("SELECT PostGIS_Version();")
        postgis_version = cursor.fetchone()[0]
        print(f"✓ PostGIS Version: {postgis_version}")
        
        # List tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        tables = cursor.fetchall()
        print(f"✓ Tables created: {len(tables)}")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Insert sample data
        print("\n📊 Inserting sample data...")
        
        # Insert a sample geo record
        cursor.execute("""
            INSERT INTO dim_geo (geo_id, latitude_centroid, longitude_centroid, area_sq_km, geom)
            VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            ON CONFLICT (geo_id) DO NOTHING
        """, ('TEST001', 30.2672, -97.7431, 1.5, -97.7431, 30.2672))
        
        conn.commit()
        print("✓ Sample geo record inserted")
        
        # Test spatial query
        cursor.execute("""
            SELECT geo_id, ST_AsText(geom) as point
            FROM dim_geo
            WHERE ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint(-97.74, 30.27), 4326)::geography,
                5000
            )
            LIMIT 5
        """)
        nearby = cursor.fetchall()
        print(f"✓ Spatial query test: Found {len(nearby)} nearby points")
        
        cursor.close()
        conn.close()
        
        return True
        
    except ImportError:
        print("⚠ psycopg2 not installed. Install with: pip install psycopg2-binary")
        return False
    except Exception as e:
        print(f"❌ PostgreSQL Error: {e}")
        return False


# ============================================
# MinIO Test
# ============================================
def test_minio():
    """Test MinIO S3-compatible storage"""
    print("\n" + "=" * 50)
    print("📦 Testing MinIO (S3-compatible)")
    print("=" * 50)
    
    try:
        from minio import Minio
        from minio.error import S3Error
        
        client = Minio(
            "localhost:9000",
            access_key="safebeat_admin",
            secret_key="safebeat_secret_2024",
            secure=False
        )
        
        # Create buckets
        buckets = ['raw-data', 'cleaned-data', 'models', 'reports']
        
        for bucket in buckets:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"✓ Created bucket: {bucket}")
            else:
                print(f"✓ Bucket exists: {bucket}")
        
        # Upload a test file
        test_content = f"SafeBeat test file - {datetime.now()}"
        test_file = "test_upload.txt"
        
        with open(test_file, 'w') as f:
            f.write(test_content)
        
        client.fput_object("raw-data", "test/test_upload.txt", test_file)
        print(f"✓ Test file uploaded to raw-data/test/")
        
        # List objects
        objects = list(client.list_objects("raw-data", recursive=True))
        print(f"✓ Objects in raw-data: {len(objects)}")
        
        # Cleanup
        import os
        os.remove(test_file)
        
        return True
        
    except ImportError:
        print("⚠ minio not installed. Install with: pip install minio")
        return False
    except Exception as e:
        print(f"❌ MinIO Error: {e}")
        return False


# ============================================
# Airflow Test
# ============================================
def test_airflow():
    """Test Airflow API"""
    print("\n" + "=" * 50)
    print("🌪️ Testing Apache Airflow")
    print("=" * 50)
    
    try:
        import requests
        from requests.auth import HTTPBasicAuth
        
        # Check Airflow health
        response = requests.get(
            "http://localhost:8080/health",
            timeout=10
        )
        
        if response.status_code == 200:
            health = response.json()
            print(f"✓ Airflow Webserver: {health.get('metadatabase', {}).get('status', 'OK')}")
            print(f"✓ Scheduler: {health.get('scheduler', {}).get('status', 'OK')}")
        else:
            print(f"⚠ Airflow responded with status: {response.status_code}")
        
        # Check DAGs
        response = requests.get(
            "http://localhost:8080/api/v1/dags",
            auth=HTTPBasicAuth('admin', 'safebeat_admin'),
            timeout=10
        )
        
        if response.status_code == 200:
            dags = response.json().get('dags', [])
            print(f"✓ DAGs available: {len(dags)}")
            for dag in dags:
                print(f"   - {dag['dag_id']}: {'Active' if not dag['is_paused'] else 'Paused'}")
        
        return True
        
    except requests.exceptions.ConnectionError:
        print("⚠ Airflow not ready yet. Wait a few minutes and try again.")
        return False
    except Exception as e:
        print(f"❌ Airflow Error: {e}")
        return False


# ============================================
# Load Sample Data to PostgreSQL
# ============================================
def load_sample_data():
    """Load sample data from local parquet files to PostgreSQL"""
    print("\n" + "=" * 50)
    print("📤 Loading Sample Data to PostgreSQL")
    print("=" * 50)
    
    try:
        import psycopg2
        from psycopg2.extras import execute_values
        
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="safebeat",
            user="safebeat_user",
            password="safebeat_db_2024"
        )
        cursor = conn.cursor()
        
        # Load risk scores
        risk_scores = pd.read_csv(
            r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\festival_risk_scores.csv'
        )
        
        print(f"✓ Loaded {len(risk_scores)} festival risk scores")
        
        # Insert to analysis_risk_scores (simplified - no FK constraint for test)
        cursor.execute("TRUNCATE TABLE analysis_risk_scores CASCADE")
        
        for _, row in risk_scores.head(20).iterrows():  # Only first 20 for test
            cursor.execute("""
                INSERT INTO analysis_risk_scores 
                (event_id, event_name, incident_count, avg_priority, risk_score, risk_category, has_alcohol)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    risk_score = EXCLUDED.risk_score,
                    risk_category = EXCLUDED.risk_category
            """, (
                int(row.get('event_id', 0)) if pd.notna(row.get('event_id')) else 0,
                row.get('event_name', 'Unknown')[:500],
                int(row.get('incident_count', 0)),
                float(row.get('avg_priority', 0)),
                float(row.get('risk_score', 0)),
                row.get('risk_category', 'LOW'),
                bool(row.get('has_alcohol', False))
            ))
        
        conn.commit()
        print(f"✓ Inserted 20 festival risk scores to PostgreSQL")
        
        # Verify
        cursor.execute("SELECT COUNT(*) FROM analysis_risk_scores")
        count = cursor.fetchone()[0]
        print(f"✓ Verification: {count} records in analysis_risk_scores")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return False


# ============================================
# Main
# ============================================
if __name__ == "__main__":
    print("🎵 SafeBeat Infrastructure Test")
    print("=" * 50)
    print(f"Started at: {datetime.now()}")
    
    results = {}
    
    results['postgres'] = test_postgres()
    results['minio'] = test_minio()
    results['airflow'] = test_airflow()
    
    if results['postgres']:
        results['data_load'] = load_sample_data()
    
    print("\n" + "=" * 50)
    print("📋 TEST SUMMARY")
    print("=" * 50)
    
    for service, status in results.items():
        icon = "✅" if status else "❌"
        print(f"{icon} {service.upper()}: {'PASSED' if status else 'FAILED'}")
    
    print("\n🌐 Access URLs:")
    print("   MinIO Console: http://localhost:9001")
    print("   Airflow UI: http://localhost:8080")
    print("   PostgreSQL: localhost:5432")
