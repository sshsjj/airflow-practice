from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from neural_style.neural_style import stylize

# default arguments for each task
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('airFlow_pset',
          default_args=default_args)  # "schedule_interval=None" means this dag will only be run by external commands


import final_utils.helper as helper
class args:
    content_image = './data/luigi.jpg'
    model = './model/mosaic.pth'
    output_image = '/Users/sshen/PycharmProjects/Airflow_test/output/luigi.jpg'
    content_scale = None
    cuda = 0

download_images = PythonOperator(
    task_id='download_images',
    python_callable=helper.download_s3_files,
    op_kwargs={'bucket_nm': 'cscie29-data', 's3_path' : 'pset4/data/', 'copy_to': '/Users/sshen/PycharmProjects/Airflow_test/data/'},
    dag=dag)

download_models = PythonOperator(
    task_id='download_models',
    python_callable=helper.download_s3_files,
    op_kwargs={'bucket_nm': 'cscie29-data', 's3_path' : 'pset4/model/', 'copy_to':'/Users/sshen/PycharmProjects/Airflow_test/model/'},
    dag=dag)

sleep_task = BashOperator(
    task_id='sleep_for_1',
    bash_command='sleep 5',
    dag=dag)


styling = PythonOperator(
    task_id='styling',
    python_callable=stylize,
    # op_kwargs={args: args},
    op_kwargs={'args':args},
    dag=dag)


[download_images, download_models] >> sleep_task >> styling

