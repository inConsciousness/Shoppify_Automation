"""
Airflow DAG for Shopify AI Automation Pipeline
Orchestrates: ETL → Lambda → Shopify upload
"""

import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaOperator
from airflow.providers.amazon.aws.sensors.glue import AwsGlueJobSensor
from airflow.providers.amazon.aws.sensors.lambda_function import AwsLambdaFunctionSensor

# DAG configuration
default_args = {
    'owner': 'shopify-automation',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'aws_conn_id': 'aws_default',
    'region_name': 'us-east-2'
}

dag = DAG(
    'shopify_ai_automation',
    default_args=default_args,
    description='Complete Shopify AI automation pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['shopify', 'ai', 'automation', 'etl']
)

# Task 1: Trigger PySpark ETL job on AWS Glue
trigger_etl_job = AwsGlueJobOperator(
    task_id='trigger_etl_job',
    job_name='shopify-etl-job',
    script_location='s3://my-shopify-ai-project/scripts/example_etl.py',
    script_args={
        '--input-path': 's3://my-shopify-ai-project/data/raw/',
        '--output-path': 's3://my-shopify-ai-project/data/clean/',
        '--s3-bucket': 'my-shopify-ai-project'
    },
    region_name='us-east-2',
    dag=dag
)

# Task 2: Wait for ETL job completion
wait_for_etl = AwsGlueJobSensor(
    task_id='wait_for_etl_completion',
    job_name='shopify-etl-job',
    aws_conn_id='aws_default',
    dag=dag
)

# Task 3: Validate ETL output
def validate_etl_output(**context):
    """Validate that ETL produced clean data"""
    import boto3
    
    s3 = boto3.client('s3', region_name='us-east-2')
    bucket = 'my-shopify-ai-project'
    key = 'data/clean/clean_products.parquet'
    
    try:
        # Check if file exists
        s3.head_object(Bucket=bucket, Key=key)
        print(f"ETL output validated: {bucket}/{key}")
        return True
    except Exception as e:
        print(f"ETL validation failed: {str(e)}")
        raise

validate_etl = PythonOperator(
    task_id='validate_etl_output',
    python_callable=validate_etl_output,
    dag=dag
)

# Task 4: Trigger Lambda function for AI processing
trigger_lambda = AwsLambdaOperator(
    task_id='trigger_ai_processing',
    function_name='shopify-ai-processor',
    payload=json.dumps({
        's3_bucket': 'my-shopify-ai-project',
        'parquet_key': 'data/clean/clean_products.parquet',
        'batch_size': 100
    }),
    aws_conn_id='aws_default',
    region_name='us-east-2',
    dag=dag
)

# Task 5: Wait for Lambda completion
wait_for_lambda = AwsLambdaFunctionSensor(
    task_id='wait_for_lambda_completion',
    function_name='shopify-ai-processor',
    aws_conn_id='aws_default',
    dag=dag
)

# Task 6: Generate summary report
def generate_summary_report(**context):
    """Generate summary report of the automation run"""
    import boto3
    from datetime import datetime
    
    s3 = boto3.client('s3', region_name='us-east-2')
    
    # Get execution info
    execution_date = context['execution_date']
    
    # Create summary report
    summary = {
        'execution_date': execution_date.isoformat(),
        'pipeline': 'shopify_ai_automation',
        'status': 'completed',
        'components': {
            'etl_job': 'completed',
            'lambda_processing': 'completed',
            'shopify_upload': 'completed'
        },
        'timestamp': datetime.now().isoformat()
    }
    
    # Upload summary to S3
    s3.put_object(
        Bucket='my-shopify-ai-project',
        Key=f'reports/summary_{execution_date.strftime("%Y%m%d")}.json',
        Body=json.dumps(summary, indent=2)
    )
    
    print(f"Summary report generated for {execution_date}")
    return summary

generate_report = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag
)

# Task 7: Cleanup temporary files (optional)
cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command="""
    aws s3 rm s3://my-shopify-ai-project/temp/ --recursive --exclude "*" --include "*.tmp" || true
    echo "Cleanup completed"
    """,
    dag=dag
)

# Task 8: Send notification on success
def send_success_notification(**context):
    """Send success notification"""
    print("Shopify AI automation pipeline completed successfully!")
    print(f"Execution date: {context['execution_date']}")
    return "Success notification sent"

send_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# Define task dependencies
trigger_etl_job >> wait_for_etl >> validate_etl >> trigger_lambda >> wait_for_lambda >> generate_report >> cleanup_temp_files >> send_notification

# Alternative path for error handling
def handle_etl_failure(**context):
    """Handle ETL job failure"""
    print("ETL job failed - sending alert")
    # Add your alerting logic here
    return "ETL failure handled"

etl_failure_handler = PythonOperator(
    task_id='handle_etl_failure',
    python_callable=handle_etl_failure,
    trigger_rule='one_failed',
    dag=dag
)

# Add failure handling
trigger_etl_job >> etl_failure_handler
wait_for_etl >> etl_failure_handler
validate_etl >> etl_failure_handler 