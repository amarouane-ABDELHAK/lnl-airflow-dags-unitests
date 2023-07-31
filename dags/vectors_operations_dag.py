from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator
from hello_operator import HelloOperator
from vector_operator import VectorOperator

with DAG(
        dag_id="vector_operations",
        start_date=pendulum.today("UTC").add(days=-1),
        schedule=None,

        description="This DAG generate custom operator",
        tags=["Custom", "Operator", "Vectors"]

) as dag:
    end = EmptyOperator(
        task_id="end"
    )
    start = EmptyOperator(
        task_id="start"
    )
    say_hello = HelloOperator(task_id="sample-task", name="Abdelhak Marouane")
    add_vectors = VectorOperator(
        task_id="add_vectors",
        vector1=(3, 1),
        vector2=(4, 2),
        action="addition"
    )

    multiply_vectors = VectorOperator(
        task_id="subtract_vectors",
        vector1=(3, 1),
        vector2=(4, 2),
        action="subtraction"
    )

    start >> say_hello >> add_vectors >> multiply_vectors >> end
