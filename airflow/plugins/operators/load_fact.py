from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql = """
    INSERT INTO songplays (
        start_time,
           user_id,
             level,
           song_id,
         artist_id,
        session_id,
          location,
        user_agent
    )
    {}
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

        super(LoadFactOperator, self).__init__(*args, **kwargs)
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
        self.log.info(f"Loading data to fact table {self.target_table}")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.select_sub_sql
        )
        redshift.run(formatted_sql)
