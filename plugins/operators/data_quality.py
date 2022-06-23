import logging
from multiprocessing.sharedctypes import Value
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 tests = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info('Beginning task: quality check')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for test in self.tests:
            result = redshift_hook.get_records(test.sql_query)
            if test.test_condition(result) == False:
                raise ValueError(test.fail_message)
            logging.info(test.pass_message)
        # verify records loaded into each analysis table (pass a list of tuples)
        # receive sql based test cases and expected results
        # raise exception if expected != actual, retry
        # ex test: does column contain null values?
        # "operator uses params to get tests and results, tests are not hard coded to the operator" - rubric
        # means tests and expected results are passed into the execute method via context
        # this method needs to iterate over a list of tests (objects)