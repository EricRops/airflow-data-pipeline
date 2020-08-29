from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    - Run data checks from user-input test SQL statements compared against the user-input expected results
    """
    ui_color = '#89DA59'
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_statements="",
                 *args, **kwargs):
        """
        :param redshift_conn_id: str: Redshift connection ID
        :param check_statements: dict: A dictionary of the form {"sql" : "<statement>", "expected_result" : <result>}
            - ex.  {'sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0}
            - NOTE: can include multiple dict elements to run more than 1 test
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_statements = check_statements

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        failed_tests = []
        total_errors = 0
        for check in self.check_statements:
            sql = check.get("sql")
            self.log.info("Performing the following test:")
            self.log.info(sql)
            expected_result = check.get("expected_result")
            records = redshift_hook.get_records(sql)
            self.log.info("Records from the test:")
            self.log.info(records)
            if expected_result != records[0][0]:
                total_errors = total_errors + 1
                failed_tests.append(sql)

        if total_errors > 0:
            self.log.info("Emergency!!! At least 1 data quality test failed. Failed test(s) shown below:")
            self.log.info(failed_tests)
            raise ValueError("Data quality checks failed. See above for which tests failed")
        else:
            self.log.info("Data quality checks passed :D :D :D :D :)))))))))))))))")
