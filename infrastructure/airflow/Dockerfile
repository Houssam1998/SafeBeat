# SafeBeat Custom Airflow Image
# With numpy 2.x and all ML dependencies pre-installed for PKL model compatibility

FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install numpy 2.x first, then other packages compiled for numpy 2.x
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    'numpy>=2.0,<3.0' \
    'pandas>=2.2.0' \
    'pyarrow>=17.0.0' \
    'scikit-learn>=1.5.0' \
    'minio>=7.2.0' \
    'psycopg2-binary>=2.9.0' \
    'mlxtend>=0.23.0' \
    'openpyxl>=3.1.0' \
    'matplotlib>=3.8.0' \
    'seaborn>=0.13.0'
