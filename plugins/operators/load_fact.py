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
                 method,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.method = method

    def execute(self, context):
        """
        Run SQL for loading fact table
        """
        self.log.info(f'Beginning task: load fact table ({self.table})')
        redshift = PostgresHook(self.redshift_conn_id)
        # upsert, replace, append?
        if self.method == 'replace':
            self.log.info(f'Clearing data from {self.table}')
            redshift.run(f'DELETE FROM {self.table}')
        self.log.info(f'Inserting data from staing table to {self.table}')
        redshift.run(self.sql_query)
        if self.method == 'upsert':
            clear_sql = SqlQueries.clean_up_duplicate_songplays
            self.log.info(f'Deleting duplicates from {self.table}')
            redshift.run(clear_sql)
