from airflow.plugins_manager import AirflowPlugin

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries
from helpers.test import Test

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        StageToRedshiftOperator,
        LoadFactOperator,
        LoadDimensionOperator,
        DataQualityOperator
    ]
    helpers = [
        SqlQueries,
        Test
    ]
