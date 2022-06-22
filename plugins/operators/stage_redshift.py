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
                 json,
                 method,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json = json
        self.method = method

    def execute(self, context):
        # parameters should specify where in s3 the file is loaded and what is the target table
        # must contain a templated field that allows it to load timestamped files 
        # from s3 based on execution time and run backfills
        self.log.info(f'Beginning task: copy data from s3 to redshift staging table ({self.table})')
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        # upsert, replace, append?
        if self.method == 'replace':
            self.log.info(f'Clearing data from {self.table}')
            redshift.run(f'DELETE FROM {self.table};')
        self.log.info('Copying data from S3 to Redshift')
        copy_sql = Template(SqlQueries.copy_json_from_s3)
        copy_sql.substitute(
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            json=self.json
        )
        redshift.run(copy_sql)
        if self.method == 'upsert':
            clear_sql = None
            if self.table == 'staging_events':
                clear_sql = SqlQueries.clean_up_duplicate_staging_events
            elif self.table == 'staging_songs':
                clear_sql = SqlQueries.clean_up_duplicate_staging_songs
            else:
                return
            self.log.info(f'Deleting duplicates from {self.table}')
            redshift.run(clear_sql)





