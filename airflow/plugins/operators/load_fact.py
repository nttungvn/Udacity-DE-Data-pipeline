from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 table='',
                 query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        self.log.info('Start LoadFactOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        run_query = """
            INSERT INTO {table} {query};
        """.format(table = self.table, query = self.query)
        redshift_hook.run(run_query)
        self.log.info('Done LoadFactOperator')

