from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    - This operator loads data from various staging tables into a fact table in Redshift
    - The fact table name and sql load statement are user-defined parameters
    - The only difference between this LoadFactsOperator and the LoadDimensionOperator is that
        the append_data parameter is True here, and False for the LoadDimensionOperator
    """

    ui_color = '#FF69B4'
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_load_stmt="",
                 append_data=True,
                 *args, **kwargs):
        """
        :param redshift_conn_id: str: Redshift connection ID
        :param destination_table: str: Redshift destination table name
        :param sql_load_stmt: str: SQL load statement (can be imported from another file)
        :param append_data: boolean
            - True: append data to the existing table
            - False: empty table, then load the data
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_load_stmt = sql_load_stmt
        self.append_data = append_data

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Populating {self.destination_table} table in Redshift")

        if self.append_data == True:
            sql_formatted = f"INSERT INTO {self.destination_table} {self.sql_load_stmt}"
            redshift_hook.run(sql_formatted)
        else:
            sql_formatted = f"DELETE FROM {self.destination_table}"
            redshift_hook.run(sql_formatted)
            sql_formatted = f"INSERT INTO {self.destination_table} {self.sql_load_stmt}"
            redshift_hook.run(sql_formatted)

        self.log.info(f"Loading of {self.destination_table} table complete!")
