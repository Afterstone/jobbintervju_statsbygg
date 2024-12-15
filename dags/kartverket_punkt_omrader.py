import logging
from datetime import datetime

# import kartverket
from airflow import DAG
# from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator

from config import DEFAULT_DAG_ARGS

logger = logging.getLogger(__name__)


def hello_world(**context) -> None:
    logger.info("Hello, World!")
    logger.info(f"Context: {context}, context type: {type(context)}")


_DAG_ARGS = DEFAULT_DAG_ARGS.copy()
with DAG(
    dag_id="kartverket_punkt",
    default_args=_DAG_ARGS,
    description="",
    schedule_interval='0 12 * * *',
    start_date=datetime(2024, 12, 10),
    catchup=False,
    tags=["kartverket"],
) as dag:
    hello_world_task = PythonOperator(task_id='hello_world', python_callable=hello_world, provide_context=True)
