"""
Upload ML Models (.pkl) to MinIO for Production Use
This script uploads trained models to the MinIO 'models' bucket.
"""
from minio import Minio
from pathlib import Path
import os

# MinIO Configuration
MINIO_CONFIG = {
    'endpoint': 'localhost:9000',
    'access_key': 'safebeat_admin',
    'secret_key': 'safebeat_secret_2024',
    'secure': False
}

# Models directory
MODELS_DIR = Path(__file__).parent.parent / "models"

def upload_models():
    """Upload all .pkl models to MinIO"""
    client = Minio(
        MINIO_CONFIG['endpoint'],
        access_key=MINIO_CONFIG['access_key'],
        secret_key=MINIO_CONFIG['secret_key'],
        secure=MINIO_CONFIG['secure']
    )
    
    # Ensure bucket exists
    if not client.bucket_exists('models'):
        client.make_bucket('models')
        print("Created 'models' bucket")
    
    # Find and upload all .pkl files
    pkl_files = list(MODELS_DIR.glob("*.pkl"))
    
    if not pkl_files:
        print(f"No .pkl files found in {MODELS_DIR}")
        return
    
    print(f"Found {len(pkl_files)} models to upload:")
    
    for pkl_file in pkl_files:
        object_name = f"trained/{pkl_file.name}"
        
        try:
            # Upload the file
            client.fput_object(
                'models',
                object_name,
                str(pkl_file)
            )
            
            # Get file size
            size_mb = pkl_file.stat().st_size / (1024 * 1024)
            print(f"  ✅ {pkl_file.name} ({size_mb:.2f} MB) → models/{object_name}")
            
        except Exception as e:
            print(f"  ❌ Failed to upload {pkl_file.name}: {e}")
    
    # List all models in bucket
    print("\n📦 Models in MinIO 'models' bucket:")
    for obj in client.list_objects('models', prefix='trained/', recursive=True):
        print(f"  - {obj.object_name}")
    
    print("\n✅ Model upload complete!")

if __name__ == "__main__":
    upload_models()
