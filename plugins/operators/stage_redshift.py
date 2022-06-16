from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Custom operator for staging data to Redshift
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        # parameters should specify where in s3 the file is loaded and what is the target table
        # must contain a templated field that allows it to load timestamped files 
        # from s3 based on execution time and run backfills
        self.log.info('StageToRedshiftOperator not implemented yet')
        # copy data from s3 to redshift
        aws_hook = AwsHook("aws_credentials")
        redshift_hook = PostgresHook("redshift")
        credentials = aws_hook.get_credentials()
        sql_stmt = sql.COPY_STATIONS_SQL.format(credentials.access_key, credentials.secret_key)
        redshift_hook.run(sql_stmt)





