from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
    INSERT INTO {target_table}
    {select_sub_sql}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 select_sub_sql="",
                 target_table="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.select_sub_sql = select_sub_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE {}".format(self.target_table))
        self.log.info(f"Loading data to dimension table {self.target_table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            target_table=self.target_table,
            select_sub_sql=self.select_sub_sql
        )
        redshift.run(formatted_sql)
