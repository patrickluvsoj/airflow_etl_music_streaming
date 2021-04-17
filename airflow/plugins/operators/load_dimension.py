from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''Execute an insert query into a dimension table'''
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_insert="",
                 destination_table="",
                 delete="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_insert=sql_insert
        self.destination_table=destination_table
        self.delete=delete

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete == True:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.destination_table))

        self.log.info("Inserting query into {}".format(self.destination_table))
        
        formatted_sql = """ INSERT INTO {} {}
        """.format(self.destination_table, self.sql_insert)
        redshift.run(formatted_sql)