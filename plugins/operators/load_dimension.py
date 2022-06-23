from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_query,
                 method,
                 *args, **kwargs):
        """
        Initialize LoadDimensionOperator
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_insert = method

    def execute(self, context):
        """
        Run SQL for loading dimension table
        """
        self.log.info(f'Beginning task: load dim table ({self.table})')
        redshift = PostgresHook(self.redshift_conn_id)
        if self.method == 'replace':
            self.log.info(f'Clearing data from {self.table}')
            redshift.run(f'DELETE FROM {self.table};')
        self.log.info(f'Inserting data from staing table to {self.table}')
        redshift.run(self.sql_query)
        if self.method == 'upsert':
            clear_sql = None
            if self.table == 'artists':
                clear_sql = SqlQueries.clean_up_duplicate_artists
            elif self.table == 'songs':
                clear_sql = SqlQueries.clean_up_duplicate_songs
            elif self.table == 'time':
                clear_sql = SqlQueries.clean_up_duplicate_time
            elif self.table == 'users':
                clear_sql = SqlQueries.clean_up_duplicate_users
            else:
                return
            self.log.info(f'Deleting duplicates from {self.table}')
            redshift.run(clear_sql)
