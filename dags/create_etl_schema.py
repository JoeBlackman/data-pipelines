from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.postgres_operator import PostgresOperator
from helpers import sql_queries
from helpers.sql_queries import SqlQueries
from helpers.test import Test

default_args = {
    'owner': 'udacity',
    'Depends_on_past': False,
    'Retries': 0,
    'Catchup': False,
    'email_on_retry': False
}

with DAG(
    'create-sparkify-etl-schema',
    default_args=default_args,
    description='Create the schema for Sparkify ETL dag to run',
    start_date = datetime(2019, 1, 12),
    schedule_interval='@once',
    tags=['udacity', 'data-pipelines'],
) as dag:

    start_operator = DummyOperator(
        task_id='Begin_execution', 
        dag=dag
    )

    with TaskGroup(group_id='drop_tables') as drop_tables:
        drop_artists_table = PostgresOperator(
            task_id = 'drop_artists_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.drop_artists_table
        )

        drop_songs_table = PostgresOperator(
            task_id = 'drop_songs_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.drop_songs_table
        )

        drop_songplays_table = PostgresOperator(
            task_id = 'drop_songplays_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.drop_songplays_table
        )

        drop_staging_events_table = PostgresOperator(
            task_id = 'drop_staging_events_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.drop_staging_events_table
        )

        drop_staging_songs_table = PostgresOperator(
            task_id = 'drop_staging_songs_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.drop_staging_songs_table
        )

        drop_time_table = PostgresOperator(
            task_id = 'drop_time_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.drop_time_table
        )

        drop_users_table = PostgresOperator(
            task_id = 'drop_users_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.drop_users_table
        )

    with TaskGroup(group_id='create_tables') as create_tables:
        create_artists_table = PostgresOperator(
            task_id = 'create_artists_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.create_artists_table
        )
        
        create_songs_table = PostgresOperator(
            task_id = 'create_songs_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.create_songs_table
        )
        
        create_songplays_table = PostgresOperator(
            task_id = 'create_songplays_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.create_songplays_table
        )
        
        create_staging_events_table = PostgresOperator(
            task_id='create_staging_events_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.create_staging_events_table
        )

        create_staging_songs_table = PostgresOperator(
            task_id='create_staging_songs_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.create_staging_songs_table
        )

        create_time_table = PostgresOperator(
            task_id = 'create_time_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.create_time_table
        )

        create_users_table = PostgresOperator(
            task_id = 'create_users_table',
            dag = dag,
            postgres_conn_id = 'redshift',
            sql = SqlQueries.create_users_table
        )

    end_operator = DummyOperator(
        task_id='Stop_execution', 
        dag=dag
    )

    start_operator >> drop_tables
    drop_tables >> create_tables
    create_tables >> end_operator
