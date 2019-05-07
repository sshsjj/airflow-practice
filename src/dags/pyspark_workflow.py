from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from final_utils.atomic_plus import atomic_write


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 3),
    'email': ['ssh.sjj@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

import final_utils.helper as helper

def spark_job():
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Airflow-pyspark").getOrCreate()
    df = spark.read.csv('/Users/sshen/PycharmProjects/Airflow_test/yelp_data/yelp_subset_0.csv', header=True)
    # Pyspark logic, group by stars and check how many reviews for each star
    df1 = df.groupBy('stars').count().orderBy(df.stars.desc())
    df1.coalesce(1).write.csv('/Users/sshen/PycharmProjects/Airflow_test/yelp_output/yelp_subset_0.csv')



with DAG('pyspark', default_args=default_args) as dag:
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=helper.download_s3_files,
        op_kwargs={'bucket_nm': 'cscie29-data', 's3_path' : 'pset5/yelp_data/', 'copy_to':'/Users/sshen/PycharmProjects/Airflow_test/yelp_data/'},
        )


    pyspark_job = PythonOperator(
        task_id='pyspark_job',
        python_callable=spark_job,
        )


    sleep = BashOperator(
        task_id='sleep30',
        bash_command="sleep 30",
        retries=3,
        )

    pyspark_job.set_upstream(download_data)
    download_data.set_downstream(sleep)