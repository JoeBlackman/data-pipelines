from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from string import Template
from helpers.sql_queries import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    """
    Custom operator for staging data to Redshift
    """
    ui_color = '#358140'
    template_fields = ('s3_key', )

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 aws_credentials_id,
                 s3_bucket,
                 s3_key,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        # parameters should specify where in s3 the file is loaded and what is the target table
        # must contain a templated field that allows it to load timestamped files 
        # from s3 based on execution time and run backfills
        self.log.info(f'Beginning task: copy data from s3 to {self.table}')
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info('Clearing data from destination Redshift table')
        redshift.run(f'DELETE FROM {self.table}')
        self.log.info('Copying data from S3 to Redshift')
        # rendered_key = self.s3_key.format(**context)
        #s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        fsql = Template(SqlQueries.copy_from_s3)
        fsql.substitute(
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key
        )
        #formatted_sql = StageToRedshiftOperator.copy_sql.format(
        #    self.table,
        #    s3_path,
        #    credentials.access_key,
        #    self.ignore_headers,
        #    self.delimiter
        #)
        #redshift.run(formatted_sql)
        redshift.run(fsql)

        #sql_stmt = sql.COPY_STATIONS_SQL.format(credentials.access_key, credentials.secret_key)
        #redshift.run(sql_stmt)





