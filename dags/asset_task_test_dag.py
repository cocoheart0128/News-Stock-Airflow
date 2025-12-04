from airflow.sdk import Asset, dag, task


@dag(schedule="@daily")
def extract_dag():

    @task(outlets=[Asset("extracted_data")])
    def extract_task():
        return {"a": 1, "b": 2}

    extract_task()


extract_dag()


@dag(schedule=[Asset("extracted_data")])
def transform_dag():

    @task(outlets=[Asset("transformed_data")])
    def transform_task(**context):
        data = context["ti"].xcom_pull(
            dag_id="extract_dag",
            task_ids="extract_task",
            key="return_value",
            include_prior_dates=True,
        )
        return {k: v * 2 for k, v in data.items()}

    transform_task()


transform_dag()


@dag(schedule=[Asset("transformed_data")])
def load_dag():

    @task
    def load_task(**context):
        data = context["ti"].xcom_pull(
            dag_id="transform_dag",
            task_ids="transform_task",
            key="return_value",
            include_prior_dates=True,
        )
        summed_data = sum(data.values())
        print(f"Summed data: {summed_data}")

    load_task()


load_dag()