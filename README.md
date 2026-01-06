# Snowflake Medallion Architecture for Healthcare Data (CCDA, HL7, CSV) with KPIs

This repository implements a **Medallion Architecture (Bronze → Silver → Gold)** on **Snowflake**, ingesting healthcare data from **CCDA**, **HL7**, and **CSV** sources, and generating **KPI dashboards** for clinical insights. It includes **SQL scripts**, **Python utilities**, and **Jupyter notebooks** for reproducible workflows.

---

## ✅ Project Overview
- **Bronze Layer**: Raw ingestion from CCDA, HL7, and CSV files into Snowflake.
- **Silver Layer**: Standardized, cleaned, and normalized data.
- **Gold Layer**: Business-ready tables and KPI calculations.
- **KPIs**: Readmission rates, Post-Discharge Follow-up, Medication Errors.

---

## 📁 Repo Structure
```
snowflake-medallion-project/
├── KPIs/
│   ├── GOLD_LAYER_READMISSION_KPI.ipynb
│   ├── SILVER_LAYER_READMISSION_KPI.ipynb
│   ├── KPI Post‑Discharge Follow‑up within 48 hours.txt
│   ├── KPI explanation Post‑Discharge Follow‑up within 48 hours.txt
│   ├── Medication Errors per 100 patients GOLD LAYER TABLES.txt
│   ├── Medication Errors per 100 patients Silver layer views.txt
│   └── text.txt
│
├── SQL/
│   ├── final database.sql
│   ├── RAW_AND_BRONZE_LAYERS_OF_CSV.sql
│   ├── CCDA_FINAL_ASSIGNMENT.sql
│   ├── CSV_PARSER.sql
│   ├── test.sql
│   │
│   ├── CCDA/
│   │   ├── ccda.zip
│   │   ├── ccdaparser.py
│   │   └── CCDA_PARSER Master.ipynb
│   │
│   ├── CSV/
│   │   ├── CSV.txt
│   │   └── csv_1.zip
│   │
│   ├── HL7/
│       ├── HL7.txt
│       ├── HL7_ADT_1_300.zip
│       ├── HL7_ORM_1_100.zip
│       └── HL7_ORU_1_100.zip
│
├── notebooks/
│   ├── CCDA_PARSER Master.ipynb
│   └── Post Discharge Follow up within 48 hours of the discharge notification.ipynb
│
├── README.md
└── requirements.txt
```

---

## 🔍 Data Sources
- **CCDA**: Clinical documents parsed using `ccdaparser.py` and loaded into Snowflake.
- **HL7**: ADT, ORM, ORU messages processed and staged in Bronze layer.
- **CSV**: Raw CSV files ingested into Snowflake via external stages.

---

## 🧱 Medallion Layers
- **Bronze**: Raw ingestion from S3 or local files; minimal schema enforcement.
- **Silver**: Cleaned and standardized tables; deduplication and type enforcement.
- **Gold**: KPI-ready tables for analytics and dashboards.

---

## 📊 KPIs Implemented
- **Readmission Rate** (Gold Layer)
- **Post-Discharge Follow-up within 48 hours**
- **Medication Errors per 100 patients**

Each KPI has corresponding **notebooks** and **SQL scripts** in the `KPIs/` folder.

---

## 🔐 Security
- Do **NOT** commit credentials (Snowflake, AWS keys).
- Use `.env` for secrets and add it to `.gitignore`.
- Example `.env`:
```
SNOWFLAKE_ACCOUNT=xxxx
SNOWFLAKE_USER=xxxx
AWS_ACCESS_KEY_ID=xxxx
AWS_SECRET_ACCESS_KEY=xxxx
```

---

## 🚀 Setup Instructions
### 1. Clone the repo
```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
```

### 2. Create virtual environment & install dependencies
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure Snowflake & AWS
- Fill `.env` with Snowflake and AWS credentials.
- Ensure Snowflake roles and warehouses are set up.

---

## 🔗 Snowflake & S3 Connections
Example Python connector:
```python
import os
import snowflake.connector
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
```

---

## 📦 Requirements
```
snowflake-connector-python
boto3
python-dotenv
pandas
jupyter
pytest
```

---

## 🛡️ Best Practices
- Keep `.env` and zip files out of GitHub.
- Use Snowpipe for automated ingestion.
- Apply masking policies for sensitive data.

---

## 📚 References
- [Snowflake Docs](https://docs.snowflake.com/)
- [AWS S3 Docs](https://docs.aws.amazon.com/s3/)
- [HL7 Standards](https://www.hl7.org/)
