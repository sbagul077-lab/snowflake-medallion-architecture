
# Snowflake Medallion Data Pipeline (Bronze → Silver → Gold) with AWS S3

A production-ready, version-controlled repository for a Snowflake-based data pipeline that ingests raw data from **AWS S3** into **Snowflake (Bronze)**, refines and standardizes it in **Silver**, and publishes curated analytics in **Gold**. The project includes reproducible **Snowflake notebooks**, **SQL DDL/DML**, and **Python utilities** for orchestration.

> Maintainer: **Sanket Bagul, Jivisha Eratkar, Srenivas, Akshay Patel K A**  
> Location: Pune, India  
> Tech: Snowflake, AWS S3, Snowpipe, Python, Jupyter

---

## 🎯 Goals
- Reliable ingestion from S3 to Snowflake using Snowpipe/Stage COPY.
- Clear, testable transformations across Medallion layers.
- Secure secrets handling (no credentials in Git!).
- Reproducible notebooks for exploration and validation.

---

## 📁 Repo Structure
```
snowflake-project/
├── notebooks/                # Jupyter notebooks (.ipynb)
│   ├── 01_ingest_bronze.ipynb
│   ├── 02_transform_silver.ipynb
│   ├── 03_publish_gold.ipynb
│   └── utils_demo.ipynb
├── sql/                      # SQL scripts (DDL/DML)
│   ├── 00_init_database.sql
│   ├── 10_bronze_objects.sql
│   ├── 20_silver_objects.sql
│   ├── 30_gold_objects.sql
│   └── 99_teardown.sql
├── src/                      # Python modules/utilities
│   ├── s3_client.py
│   ├── snowflake_client.py
│   ├── snowpipe_utils.py
│   └── transforms/
├── tests/                    # Unit tests
├── README.md                 # Project documentation
├── requirements.txt          # Python dependencies
└── .gitignore                # Ignore notebooks caches & secrets
```

---

## 🔐 Security & Secrets

# Snowflake
SNOWFLAKE_ACCOUNT=xxxxxxxxx
SNOWFLAKE_USER=your_user
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MEDALLION_DB
SNOWFLAKE_SCHEMA=BRONZE
SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/snowflake_key.pem
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase

# AWS
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=my-bucket
S3_PREFIX=landing/



## 🚀 Quick Start

### 1) Clone & set up
```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp config/sample.env .env   # Fill in your local secrets
```

### 2) Create Snowflake objects
Run the DDL scripts in `sql/` using Snowflake Web UI or a client:
```sql
-- 00_init_database.sql
CREATE DATABASE IF NOT EXISTS MEDALLION_DB;
CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.GOLD;
```

### 3) Configure Stage & Snowpipe (S3 → Snowflake)
Use `snowflake_client.py` or execute scripts that:
- Create an **external stage** pointing to your S3 bucket.
- Define a **file format** (e.g., JSON/CSV/Parquet).
- Create **Snowpipe** with auto-ingest from S3 events (or run `COPY INTO`).

Example SQL (simplified):
```sql
CREATE OR REPLACE FILE FORMAT ff_json TYPE = JSON;
CREATE OR REPLACE STAGE s3_stage
  URL='s3://<bucket>/<prefix>'
  CREDENTIALS=(AWS_KEY_ID='...' AWS_SECRET_KEY='...')
  FILE_FORMAT=ff_json;

#XML File Format
CREATE OR REPLACE FILE FORMAT FF_XML
  TYPE = XML
  DISABLE_AUTO_CONVERT = TRUE;

#JSON File Format
CREATE OR REPLACE FILE FORMAT HL7_BRONZE_FF
  TYPE = 'CSV'
  FIELD_DELIMITER = '\t'
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 0
  NULL_IF = ('', 'NULL')
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE;

#CSV File Format
> Prefer IAM role-based auth or Snowflake external stages with AWS integration for production.

---

## 🧱 Medallion Layers
- **Bronze**: Raw, immutable ingestion from S3; minimal schema enforcement.
- **Silver**: Cleaned/standardized data; deduped, typed columns, basic QA.
- **Gold**: Business-ready marts; dimensional models, aggregates, dashboards.

---

## 🔗 Snowflake & S3 Connections (Python)
Basic connectors used in `src/`:
```python
# snowflake_client.py
import os
import snowflake.connector

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    private_key=open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), 'rb').read()
)


## 🧪 Testing
- Use `pytest` for Python modules in `src/`.
- Add data quality checks for Silver (nulls, types, duplicates).
- Include sample test data in `/tests/data/` (non-sensitive).

---

## 📦 Requirements
Minimal `requirements.txt`:
```
snowflake-connector-python
boto3
python-dotenv
pandas
snowflake
pytest
```

---

## 🛡️ Best Practices
- Keep **DDL/DML** versioned under `sql/`.
- Treat notebooks as **reproducible experiments**; production logic belongs in `src/`.
- Use **roles/least privilege** in Snowflake and **IAM roles** in AWS.
- Prefer **external stages** + **Snowpipe** for scalable ingestion; 
  fall back to scheduled `COPY INTO` for simpler setups.
- Store PII securely; consider masking policies and row access policies.

---

## 📚 References
- Snowflake Stages & Snowpipe: https://docs.snowflake.com/en/user-guide/data-load-s3
- Snowflake Security & Access Control: https://docs.snowflake.com/en/user-guide/security-access-control
- AWS S3 & IAM: https://docs.aws.amazon.com/IAM/latest/UserGuide/access.html

---
---

## 🙋 Support
Issues and PRs are welcome. For questions, open an issue or reach out via GitHub.
