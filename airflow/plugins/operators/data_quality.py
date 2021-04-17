from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''Conduct data quality check for tables included in the tables list'''
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 null_check_dict="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.null_check_dict = null_check_dict

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
            
        #Check if tables are empty
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]

            if num_records is None or num_records < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")

        self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
        #Check for NULL values in tables
        for key in self.null_check_dict: 
            self.log.info(f"NULL Data quality check for column: {self.null_check_dict[key]} in table: {key}.")

            records = redshift.get_records(f"SELECT {self.null_check_dict[key]}\
                                            FROM {key} WHERE {self.null_check_dict[key]} IS NULL")

            if len(records) > 1:
                raise ValueError(f"Failed {records} rows are NULL for column: {self.null_check_dict[key]} for table: {key}")
                
            self.log.info(f"Data quality check passed for table {key}. No NULL values.")
       
        self.log.info("All data quality check completed.")
            