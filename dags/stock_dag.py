from airflow import DAG
from airflow.assets import load_assets

with DAG(
    dag_id="stock_asset_pipeline",
    schedule="@daily",
    start_date="2025-01-01",
    catchup=False,
    tags=["stock", "asset", "finance"]
):
    # Load asset functions from assets folder
    load_assets("assets")