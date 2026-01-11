# Airflow DAGs for Data Warehouse ETL

This directory contains Airflow DAG files generated from SQL files in `datawarehouse/sql/`.

## Generated DAGs

Total: **30 DAGs** covering all ETL workflows:

### ODS Layer (10 DAGs) - Daily at 2:00 AM
- `ods_01_order_master_etl.py` - Order master table ETL
- `ods_02_order_detail_etl.py` - Order detail table ETL
- `ods_03_customer_etl.py` - Customer master table ETL
- `ods_04_product_etl.py` - Product master table ETL
- `ods_05_production_plan_etl.py` - Production plan table ETL
- `ods_06_production_order_etl.py` - Production order table ETL
- `ods_07_bom_etl.py` - Bill of materials table ETL
- `ods_08_material_etl.py` - Material master table ETL
- `ods_09_inventory_etl.py` - Inventory table ETL
- `ods_10_purchase_etl.py` - Purchase order table ETL

### DWD Layer (7 DAGs) - Daily at 3:00 AM
- `dwd_01_order_fact_etl.py` - Order fact table ETL
- `dwd_02_production_fact_etl.py` - Production fact table ETL
- `dwd_03_inventory_fact_etl.py` - Inventory fact table ETL
- `dwd_04_purchase_fact_etl.py` - Purchase fact table ETL
- `dwd_05_quality_fact_etl.py` - Quality fact table ETL
- `dwd_06_equipment_runtime_etl.py` - Equipment runtime fact table ETL
- `dwd_07_cost_fact_etl.py` - Cost fact table ETL

### DWS Layer (7 DAGs) - Daily at 4:00 AM
- `dws_01_order_daily_etl.py` - Order daily summary ETL
- `dws_02_production_daily_etl.py` - Production daily summary ETL
- `dws_03_inventory_daily_etl.py` - Inventory daily summary ETL
- `dws_04_purchase_daily_etl.py` - Purchase daily summary ETL
- `dws_05_quality_daily_etl.py` - Quality daily summary ETL
- `dws_06_equipment_runtime_daily_etl.py` - Equipment runtime daily summary ETL
- `dws_07_cost_daily_etl.py` - Cost daily summary ETL

### ADS Layer (6 DAGs) - Daily at 5:00 AM
- `ads_01_sales_analysis_etl.py` - Sales analysis report ETL
- `ads_02_production_analysis_etl.py` - Production analysis report ETL
- `ads_03_inventory_analysis_etl.py` - Inventory analysis report ETL
- `ads_04_purchase_analysis_etl.py` - Purchase analysis report ETL
- `ads_05_quality_analysis_etl.py` - Quality analysis report ETL
- `ads_06_business_overview_etl.py` - Business overview report ETL

## Setup

### 1. Configure MySQL Connection in Airflow

Before running the DAGs, configure the MySQL connection in Airflow:

1. Access Airflow Web UI: http://localhost:8080
2. Go to **Admin** → **Connections**
3. Add a new connection:
   - **Connection Id**: `mysql_default`
   - **Connection Type**: `MySQL`
   - **Host**: `mysql-db` (or your MySQL host)
   - **Schema**: `sqlExpert`
   - **Login**: `sqluser`
   - **Password**: `sqlpass123`
   - **Port**: `3306`

### 2. SQL Files Location

SQL files are mounted to `/opt/airflow/dags/sql/` in the Airflow container.
The DAGs will automatically read SQL files from this location.

### 3. DAG File Location

DAG files should be placed in the Airflow dags directory:
- Local path: `datawarehouse/scheduler/airflow/dags/`
- Container path: `/opt/airflow/dags/` (mounted from `data_volume/airflow/dags`)

## Regenerating DAGs

To regenerate DAG files from SQL files:

```bash
# Run the generation script in python-scripts container
docker exec python-scripts python /workspace/datawarehouse/scheduler/airflow/generate_dags.py
```

Or manually:

```bash
cd datawarehouse/scheduler/airflow
python generate_dags.py
```

## DAG Structure

Each DAG file:
- Reads SQL content from the corresponding SQL file
- Uses `MySqlOperator` to execute SQL
- Has scheduled execution (cron format)
- Includes retry logic and error handling
- Tagged by layer (ods, dwd, dws, ads)

## Schedule

- **ODS Layer**: Daily at 2:00 AM (`0 2 * * *`)
- **DWD Layer**: Daily at 3:00 AM (`0 3 * * *`)
- **DWS Layer**: Daily at 4:00 AM (`0 4 * * *`)
- **ADS Layer**: Daily at 5:00 AM (`0 5 * * *`)

## Notes

1. All DAGs are paused by default (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`)
2. Enable DAGs manually in Airflow Web UI before they start running
3. Make sure MySQL connection is configured correctly
4. SQL files must be accessible at `/opt/airflow/dags/sql/` in the container
