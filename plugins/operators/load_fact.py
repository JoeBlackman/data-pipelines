from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        """
        Run SQL for loading fact table
        """
        self.log.info('Beginning task: load fact table')
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info('Clearing data from destination Redshift table')
        redshift.run(f'DELETE FROM {self.table}')
        self.log.info('Inserting data from staing table to analysis table')
        redshift.run(self.sql_query)