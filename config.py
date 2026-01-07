import os
from pathlib import Path

# Базовые пути
BASE_DIR = Path(__file__).parent

MINIO_CONFIG = {
    # Настройки MinIO
    'endpoint_url': os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
    'access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
    'secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
    'bucket_name': os.getenv('MINIO_BUCKET', 'bucketmaks'),

    # Локальные папки (относительно BASE_DIR)
    'input_folder': str(BASE_DIR / 'data' / 'input'),
    'temp_folder': str(BASE_DIR / 'data' / 'temp'),
    'archive_folder': str(BASE_DIR / 'data' / 'archive'),
    'log_file': str(BASE_DIR / 'logs' / 'minio_pipeline.log'),

    # Настройки пайплайна
    'max_concurrent_files': int(os.getenv('MAX_CONCURRENT_FILES', '5')),
    's3_prefix': os.getenv('S3_PREFIX', 'processed-data'),

    # Условия фильтрации
    'filter_conditions': {
        'age_range': {
            'min': int(os.getenv('AGE_MIN', '18')),
            'max': int(os.getenv('AGE_MAX', '40'))
        }
    },

    # Настройки версионирования
    'log_s3_key': 'pipeline.log',
    'bucket_versioning': os.getenv('BUCKET_VERSIONING', 'Enabled') # Возможные значения: 'Enabled', 'Suspended', 'Disabled'
}