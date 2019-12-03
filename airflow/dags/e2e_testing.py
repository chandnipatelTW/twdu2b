from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow import LoggingMixin
from airflow.models import Variable

logger = LoggingMixin()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today().strftime('%Y-%m-%d'),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('e2e_testing',
          default_args=default_args,
          schedule_interval='0 * * * *',
          catchup=False)

initiliaze_e2e_script = "e2e_test.sh"

task_execute_e2e = BashOperator(task_id='execute_e2e_test',
                                bash_command=initiliaze_e2e_script,
                                dag=dag)
