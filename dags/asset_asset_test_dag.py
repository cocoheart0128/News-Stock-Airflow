from airflow.sdk import asset


@asset(schedule="@daily")
def extracted_data():
    return {"a": 1, "b": 2,"c":5}


@asset(schedule=extracted_data)
def transformed_data(context):

    data = context["ti"].xcom_pull(
        dag_id="extracted_data",
        task_ids="extracted_data",
        key="return_value",
        include_prior_dates=True,
    )
    return {k: v * 2 for k, v in data.items()}


@asset(schedule=transformed_data)
def loaded_data(context):

    data = context["task_instance"].xcom_pull(
        dag_id="transformed_data",
        task_ids="transformed_data",
        key="return_value",
        include_prior_dates=True,
    )
    summed_data = sum(data.values())
    print(f"Summed data: {summed_data}")
