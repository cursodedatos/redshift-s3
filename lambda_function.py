import boto3
import csv

def lambda_handler(event, context):
    print (event)
    # Configuración
    REGION = 'us-east-1'
    S3_PATH = 's3://xxxxx/bank.csv'
    REDSHIFT_WORKGROUP = 'xxxxx'
    REDSHIFT_DATABASE = 'dev'
    REDSHIFT_TABLE = 'bank'
    iam_role_arn = 'arn:aws:iam::xxxxxx:role/service-role/AmazonRedshift-CommandsAccessRole-xxxxx'

    # Inicializar cliente boto3
    client = boto3.client('redshift-data', region_name=REGION)

    # Comando COPY
    copy_sql = f"""
    COPY {REDSHIFT_TABLE}
    FROM '{S3_PATH}'
    IAM_ROLE '{iam_role_arn}'
    DELIMITER ';'
    IGNOREHEADER 1
    REGION '{REGION}'
    TIMEFORMAT 'auto'
    FORMAT AS CSV;
    """

    response = client.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=copy_sql,
        SecretArn= "arn:aws:secretsmanager:us-east-1:xxxxxx:secret:cred-redshift-xxxxx"
    )
    print("Carga iniciada. Statement ID:", response['Id'])         

    
    return str(response['Id'])
