import logging
from multiprocessing.sharedctypes import Value
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) <1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f"Data quality check on {self.table} passed with {records[0][0]} records")
        # verify records loaded into each analysis table (pass a list of analysis tables?)
        # receive sql based test cases and expected results
        # raise exception if expected != actual, retry
        # ex test: does column contain null values?
        # "operator uses params to get tests and results, tests are not hard coded to the operator" - rubric
        # means tests and expected results are passed into the execute method via context
        # this method needs to iterate over a list/dictionary of tests
        # what is a test?
        # tuple with sql and expected result?