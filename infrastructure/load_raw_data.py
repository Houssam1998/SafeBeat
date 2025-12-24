"""
SafeBeat - Complete Data Loader with Raw Data
Uploads ALL data including raw Excel files to MinIO
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import os
from datetime import datetime

CONFIG = {
    'minio': {
        'endpoint': 'localhost:9000',
        'access_key': 'safebeat_admin',
        'secret_key': 'safebeat_secret_2024',
    },
    'local_paths': {
        'root': r'd:\uemf\s9\Data mining\SafeBeat\datasets',
        'cleaned': r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned',
        'analysis': r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis',
        'enriched': r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched',
        'models': r'd:\uemf\s9\Data mining\SafeBeat\models'
    }
}


def upload_raw_data():
    """Upload raw Excel/Parquet files to MinIO raw-data bucket"""
    print("\n" + "=" * 50)
    print("📦 Uploading RAW DATA to MinIO")
    print("=" * 50)
    
    try:
        from minio import Minio
        
        client = Minio(
            CONFIG['minio']['endpoint'],
            access_key=CONFIG['minio']['access_key'],
            secret_key=CONFIG['minio']['secret_key'],
            secure=False
        )
        
        # Ensure bucket exists
        if not client.bucket_exists('raw-data'):
            client.make_bucket('raw-data')
        
        root_path = CONFIG['local_paths']['root']
        uploaded = 0
        
        # Upload raw files
        raw_files = [
            ('APD_911_Calls_for_Service_2023-2025_20251223.xlsx', '911/'),
            ('ACE_Events_20251223.xlsx', 'events/'),
        ]
        
        for filename, prefix in raw_files:
            filepath = os.path.join(root_path, filename)
            if os.path.exists(filepath):
                size_mb = os.path.getsize(filepath) / (1024*1024)
                print(f"Uploading {filename} ({size_mb:.1f} MB)...")
                
                try:
                    client.fput_object('raw-data', f"{prefix}{filename}", filepath)
                    print(f"✓ Uploaded: {filename}")
                    uploaded += 1
                except Exception as e:
                    print(f"⚠ Error uploading {filename}: {e}")
            else:
                print(f"⚠ File not found: {filename}")
        
        # Upload Foursquare parquet files
        foursquare_files = ['Foursquare_pairs.parquet', 'Foursquare_train.parquet']
        for filename in foursquare_files:
            filepath = os.path.join(root_path, filename)
            if os.path.exists(filepath):
                size_mb = os.path.getsize(filepath) / (1024*1024)
                print(f"Uploading {filename} ({size_mb:.1f} MB)...")
                
                try:
                    client.fput_object('raw-data', f"foursquare/{filename}", filepath)
                    print(f"✓ Uploaded: {filename}")
                    uploaded += 1
                except Exception as e:
                    print(f"⚠ Error: {e}")
        
        print(f"\n✅ Raw data uploaded: {uploaded} files")
        return uploaded
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 0


def upload_cleaned_data():
    """Upload cleaned parquet files"""
    print("\n" + "=" * 50)
    print("📦 Uploading CLEANED DATA to MinIO")
    print("=" * 50)
    
    try:
        from minio import Minio
        
        client = Minio(
            CONFIG['minio']['endpoint'],
            access_key=CONFIG['minio']['access_key'],
            secret_key=CONFIG['minio']['secret_key'],
            secure=False
        )
        
        if not client.bucket_exists('cleaned-data'):
            client.make_bucket('cleaned-data')
        
        uploaded = 0
        
        # Upload cleaned parquet files
        cleaned_path = CONFIG['local_paths']['cleaned']
        if os.path.exists(cleaned_path):
            for filename in os.listdir(cleaned_path):
                if filename.endswith(('.parquet', '.csv')):
                    filepath = os.path.join(cleaned_path, filename)
                    client.fput_object('cleaned-data', f"cleaned/{filename}", filepath)
                    print(f"✓ {filename}")
                    uploaded += 1
        
        # Upload enriched parquet files
        enriched_path = CONFIG['local_paths']['enriched']
        if os.path.exists(enriched_path):
            for filename in os.listdir(enriched_path):
                if filename.endswith(('.parquet', '.csv')):
                    filepath = os.path.join(enriched_path, filename)
                    client.fput_object('cleaned-data', f"enriched/{filename}", filepath)
                    print(f"✓ {filename}")
                    uploaded += 1
        
        # Upload analysis files
        analysis_path = CONFIG['local_paths']['analysis']
        if os.path.exists(analysis_path):
            for filename in os.listdir(analysis_path):
                if filename.endswith(('.parquet', '.csv')):
                    filepath = os.path.join(analysis_path, filename)
                    client.fput_object('cleaned-data', f"analysis/{filename}", filepath)
                    print(f"✓ {filename}")
                    uploaded += 1
        
        print(f"\n✅ Cleaned data uploaded: {uploaded} files")
        return uploaded
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 0


def list_minio_contents():
    """List all contents in MinIO"""
    print("\n" + "=" * 50)
    print("📋 MinIO Contents")
    print("=" * 50)
    
    try:
        from minio import Minio
        
        client = Minio(
            CONFIG['minio']['endpoint'],
            access_key=CONFIG['minio']['access_key'],
            secret_key=CONFIG['minio']['secret_key'],
            secure=False
        )
        
        for bucket in ['raw-data', 'cleaned-data', 'models', 'reports']:
            if client.bucket_exists(bucket):
                objects = list(client.list_objects(bucket, recursive=True))
                total_size = sum(obj.size for obj in objects) / (1024*1024)
                print(f"\n🗂️ {bucket}: {len(objects)} files ({total_size:.1f} MB)")
                
                for obj in objects[:5]:  # Show first 5
                    print(f"   └─ {obj.object_name}")
                if len(objects) > 5:
                    print(f"   └─ ... and {len(objects)-5} more")
                    
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    print("🎵 SafeBeat Complete Data Loader")
    print("=" * 50)
    print(f"Started at: {datetime.now()}")
    print("\n⚠️ This will upload large files (may take several minutes)")
    
    # Upload raw data (large files)
    raw_count = upload_raw_data()
    
    # Upload cleaned data
    cleaned_count = upload_cleaned_data()
    
    # Show contents
    list_minio_contents()
    
    print("\n" + "=" * 50)
    print("✅ COMPLETE DATA LOAD FINISHED")
    print("=" * 50)
