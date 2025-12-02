# News-Stock-Airflow


docker pd

<img width="1511" height="157" alt="스크린샷 2025-12-02 오후 4 23 15" src="https://github.com/user-attachments/assets/46ffc311-3014-428f-b43c-f6c7521411a2" />

docker exec -it cicd-airflow-scheduler-1 bash
airflow backfill create --dag-id dag_yfinance_pipeline --from-date 2025-11-20 --to-date 2025-11-30
airflow db clean --clean-before-timestamp '2026-01-01 00:00:00+01:00'
airflow tasks clear dag_yfinance_pipeline --start-date 2025-11-20 --end-date 2025-12-30 --yes

exit
