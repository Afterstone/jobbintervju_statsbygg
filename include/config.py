from datetime import timedelta
from typing import Any, Dict

OUTPUT_DIR = "/opt/airflow/data/"

DEFAULT_DAG_ARGS: Dict[str, Any] = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
