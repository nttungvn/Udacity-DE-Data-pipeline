from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id='',
                 table='',
                 s3_folder='',
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_folder = s3_folder
        self.json_path = json_path


    def execute(self, context):
        self.log.info('Start StageToRedshiftOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        query = """
                COPY {table}
                FROM {s3_folder}
                {json_path};
            """.format(table = self.table, s3_folder = self.s3_folder, json_path = self.json_path)
        redshift_hook.run(query)
        self.log.info('Done StageToRedshiftOperator')






