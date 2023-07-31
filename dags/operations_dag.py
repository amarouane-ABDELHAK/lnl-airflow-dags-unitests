from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def addition_task(ti, a: int = 10, b: int = 90):

    if isinstance(a, int) and isinstance(b, int):
        print(f"Got {a=} {b=}")
        result = a + b
        ti.xcom_push(key="result_of_addition", value=result)
        print(result)
        return result
    raise Exception(f"We only accept natural numbers")


def multipy_by_10(ti):
    first_par = ti.xcom_pull(task_ids="addition", key="result_of_addition")
    print("I was triggered")
    result = first_par * 10
    print(f"{result=}")
    return result


def result_sqr_root(ti):
    multiplication_result = ti.xcom_pull(task_ids="multiplication")
    print("I was triggered")
    multiplication_result *= multiplication_result
    print(f"{multiplication_result=}")
    return multiplication_result


with DAG(
        dag_id="make_operations",
        start_date=pendulum.today("UTC").add(days=-1),
        schedule=None,

        description="This DAG make operations",
        tags=["Kris", "Operations", "Addition", "Multiplication"]

) as dag:
    end = EmptyOperator(
        task_id="end"
    )
    start = EmptyOperator(
        task_id="start"
    )

    addition = PythonOperator(
        task_id="addition",
        python_callable=addition_task,
        op_kwargs={"a": 20, "b": 180}
    )
    multiplication = PythonOperator(
        task_id="multiplication",
        python_callable=multipy_by_10
    )
    sqrt = PythonOperator(
        task_id="square_root",
        python_callable=result_sqr_root
    )

    start >> addition >> multiplication >> sqrt >> end
