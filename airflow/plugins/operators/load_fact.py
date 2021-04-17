from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''Execute an insert query into a fact table'''
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_insert="",
                 destination_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_insert=sql_insert
        self.destination_table=destination_table

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Inserting query into {}".format(self.destination_table))
        
        formatted_sql = """ INSERT INTO {} {}
        """.format(self.destination_table, self.sql_insert)
        redshift.run(formatted_sql)


