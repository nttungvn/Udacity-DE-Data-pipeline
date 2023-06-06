from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 table='',
                 query='',
                 append_only = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append_only = append_only

    def execute(self, context):
        self.log.info('Start LoadDimensionOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if not self.append_only:
            self.log.info('Truncate table')
            redshift_hook.run(f"DELECT FROM {self.table}")
        run_query = """
            INSERT INTO {table} {query};
        """.format(table = self.table, query = self.query)
        redshift_hook.run(run_query)
