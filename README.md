# airflow-practice
CSCI E29 Advanced Python for Data Science final project

This project includes the knowledge learnt in class like context-manager and reused the atomic writes, binary atocmic writes, neural_style, etc.

Two workflows are implemented by using Apache Airflow:
1. workflow.py, which is similar to Pset4. 
- Download luigi.jpg and models, then apply trained styling model to the luigi.jpg
2. pyspark_workflow.py, which reads the yelp_reviews used in Pset5.
- Download yelp_reviews csv files, load into Spark and run aggregation functions and then write results to one single csv.

## Setup
- Install Apache Airflow
- Reconfigure the airflow.cfg to point the dags folder to your project folder, then Airflow can recognize it.
- Check out the code and start

## Commands
- Run workflow 'pyspark'(defined in pyspark_workflow.py) with certain a range of dates
```bash
airflow backfill pyspark -s 2019-05-01 -e 2019-05-08 
```
- Run styling operator(defined in workflow.py)
```bash
airflow test airFlow_pset styling 2019-05-03
```
More commands can be found on Airflow website: https://airflow.apache.org/tutorial.html
