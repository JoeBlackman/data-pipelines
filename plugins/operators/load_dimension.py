from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_query,
                 truncate_insert,
                 *args, **kwargs):
        """
        Initialize LoadDimensionOperator
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_insert = truncate_insert

    def execute(self, context):
        """
        Run SQL for loading dimension table
        """
        self.log.info('Beginning task: load dim table')
        redshift = PostgresHook(self.redshift_conn_id)
        if self.truncate_insert:
            self.log.info('Clearing data from destination Redshift table')
            redshift.run(f'DELETE FROM {self.table}')
        self.log.info('Inserting data from staing table to analysis table')
        redshift.run(self.sql_query)
        # insert data from staging tables to dimension table
        # done with truncate-insert pattern (target table emptied before load)
        # param that switches between modes
