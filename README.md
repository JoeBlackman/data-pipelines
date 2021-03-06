# **SUMMARY**
The purpose of this project is to create a pipeline for ETL. This will improve our ability to monitor processing steps and implement data quality checks as part of the pipeline.

# **INSTRUCTIONS**
- create an IAM user in AWS
- create a redshift cluster in region us-west-2
- in project workspace, run /opt/airflow/start.sh to start the airflow web server
    - alternatively, create an MWAA cluster on AWS
- in project workspace, click the Access Airflow button
- configure airflow with AWS credentials
- configure airflow with reshift connection
- create tables in redshift by either
    - running create_tables.sql in redshift cluster
    - run create_etl_schema dag
- run etl DAG