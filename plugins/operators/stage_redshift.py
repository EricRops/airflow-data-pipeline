from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """ This operator loads any JSON formatted files from S3 to Amazon Redshift """

    ui_color = '#358140'
    template_fields = ("s3_key",)
    staging_sql = """
        COPY {destination_table}
        FROM '{s3_path}'
        IAM_ROLE '{0}'
        JSON '{json_format}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 aws_arn_id="",
                 s3_bucket="",
                 s3_key="",
                 destination_table="",
                 json_format="auto",
                 *args, **kwargs):
        """
        :param redshift_conn_id: str: Redshift connection id (Created from run.sh script)
        :param aws_credentials_id: str: aws credentials connection id (Created from run.sh script)
        :param aws_arn_id: str: aws IAM ARN key id (Created from run.sh script)
        :param s3_bucket: str: s3 bucket name
        :param s3_key: str: s3 prefix path to where the JSON files are located
        :param destination_table: str: Redshift destination table name
        :param json_format: str:
            - use "auto" if the JSON keys match the destination_table column names EXACTLY.
            - If JSON keys DO NOT EXACTLY MATCH the destination_table column names, then:
                Input path to LOG_JSONPATH file to map the column names
                ex: "s3://udacity-dend/log_json_path.json"
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.aws_arn_id = aws_arn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.destination_table = destination_table
        self.json_format = json_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        aws_arn = Variable.get(self.aws_arn_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Staging {self.destination_table} from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.staging_sql.format(
            # credentials.access_key,
            # credentials.secret_key,
            aws_arn,
            destination_table=self.destination_table,
            s3_path=s3_path,
            json_format=self.json_format
        )
        redshift_hook.run(formatted_sql)
        self.log.info(f"Staging of {self.destination_table} complete!")
