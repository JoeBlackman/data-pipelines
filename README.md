# **SUMMARY**
The purpose of this project is to create a pipeline for ETL. This will improve our ability to monitor processing steps and implement data quality checks as part of the pipeline.
Data Source = Amazon S3 (us-west-2)
Data Destination = Amazon Redshift
Template provides sql and ETL code. Just need to implement operators and DAGs

# **INSTRUCTIONS**
- create an IAM user in AWS
- create a redshift cluster in region us-west-2
- in project workspace, run /opt/airflow/start.sh to start the airflow web server
- in project workspace, click the Access Airflow button
- configure airflow with AWS credentials
- configure airflow with reshift connection

# **MANIFEST**
