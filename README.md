# ETL Toll Data Pipeline

## Overview
This project implements an **ETL (Extract, Transform, Load) pipeline** using **Apache Airflow** to process toll data. The DAG automates the workflow of extracting data from multiple file formats, transforming it, and consolidating it into a single staging file.

---

## Features
- Extract data from:
  - CSV files
  - TSV files
  - Fixed-width text files
- Consolidate extracted data into a single CSV
- Transform vehicle type field to **uppercase**
- Fully automated using Airflow with daily scheduling
- Email notifications on failure or retries
- Retry mechanism with delay for failed tasks


---

## DAG Workflow

1. **Unzip Data**
   - Task: `unzip_data`
   - Description: Extracts `tolldata.tgz` archive into the DAGs folder.

2. **Extract Data**
   - Task: `extract_data_from_csv` → Extracts first 4 columns from CSV.
   - Task: `extract_data_from_tsv` → Extracts columns 5-7 from TSV and converts tabs to commas.
   - Task: `extract_data_from_fixed_width` → Extracts specific character positions from fixed-width text file.

3. **Consolidate Data**
   - Task: `consolidate_data`
   - Description: Combines extracted CSV, TSV, and fixed-width data into a single CSV.

4. **Transform Data**
   - Task: `transform_data`
   - Description: Converts vehicle type field to uppercase and saves final CSV in `staging/` folder.

---

## DAG Schedule
- Runs **daily** using `schedule_interval='@daily'`.
- Configured with retries (`1 retry`, `5 minutes delay`) and email notifications on failure.

---

## Airflow Configuration

```python
default_args = {
    'owner': 'Tom',
    'start_date': datetime.today(),
    'email': ['tom@test.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
