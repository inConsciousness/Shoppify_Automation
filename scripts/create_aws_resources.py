#!/usr/bin/env python3
"""
AWS Resource Creation Script for Shopify AI Automation
Creates Lambda, Glue Job, and Step Functions
"""

import boto3
import json
import os
from pathlib import Path

def load_config():
    """Load AWS resource configuration"""
    config_path = Path(__file__).parent.parent / "config" / "aws_resources.json"
    with open(config_path, 'r') as f:
        return json.load(f)

def create_lambda_function(config):
    """Create Lambda function for AI processing"""
    lambda_client = boto3.client('lambda', region_name=config['aws_resources']['region'])
    
    # Create deployment package
    lambda_dir = Path(__file__).parent.parent / "lambda"
    zip_path = Path(__file__).parent.parent / "lambda_package.zip"
    
    # Create zip file
    import zipfile
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file_path in lambda_dir.rglob('*'):
            if file_path.is_file():
                zipf.write(file_path, file_path.relative_to(lambda_dir))
    
    # Read zip file
    with open(zip_path, 'rb') as f:
        zip_content = f.read()
    
    try:
        # Create Lambda function
        response = lambda_client.create_function(
            FunctionName=config['aws_resources']['lambda_function']['name'],
            Runtime=config['aws_resources']['lambda_function']['runtime'],
            Role=config['aws_resources']['iam_role']['arn'],
            Handler='handler.lambda_handler',
            Code={'ZipFile': zip_content},
            Description='Shopify AI Product Processing Lambda',
            Timeout=config['aws_resources']['lambda_function']['timeout'],
            MemorySize=config['aws_resources']['lambda_function']['memory_size'],
            Environment={
                'Variables': {
                    'S3_BUCKET_NAME': config['aws_resources']['s3_bucket']['name'],
                    'AWS_DEFAULT_REGION': config['aws_resources']['region']
                }
            }
        )
        print(f"✅ Lambda function created: {response['FunctionArn']}")
        return response['FunctionArn']
    except lambda_client.exceptions.ResourceConflictException:
        print("⚠️  Lambda function already exists")
        return f"arn:aws:lambda:{config['aws_resources']['region']}:195275669634:function:{config['aws_resources']['lambda_function']['name']}"
    finally:
        # Clean up zip file
        if zip_path.exists():
            zip_path.unlink()

def create_glue_job(config):
    """Create Glue ETL job"""
    glue_client = boto3.client('glue', region_name=config['aws_resources']['region'])
    
    # Upload ETL script to S3
    s3_client = boto3.client('s3', region_name=config['aws_resources']['region'])
    etl_script_path = Path(__file__).parent.parent / "spark_jobs" / "example_etl.py"
    
    with open(etl_script_path, 'rb') as f:
        s3_client.put_object(
            Bucket=config['aws_resources']['s3_bucket']['name'],
            Key='scripts/example_etl.py',
            Body=f.read()
        )
    
    try:
        # Create Glue job
        response = glue_client.create_job(
            Name=config['aws_resources']['glue_job']['name'],
            Role=config['aws_resources']['iam_role']['arn'],
            Command={
                'Name': 'glueetl',
                'ScriptLocation': f"s3://{config['aws_resources']['s3_bucket']['name']}/scripts/example_etl.py",
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--job-language': 'python',
                '--job-bookmark-option': 'job-bookmark-enable',
                '--input-path': config['s3_paths']['raw_data'],
                '--output-path': config['s3_paths']['clean_data'],
                '--s3-bucket': config['aws_resources']['s3_bucket']['name']
            },
            MaxRetries=0,
            Timeout=config['aws_resources']['glue_job']['timeout'],
            WorkerType=config['aws_resources']['glue_job']['worker_type'],
            NumberOfWorkers=config['aws_resources']['glue_job']['number_of_workers'],
            GlueVersion='4.0'
        )
        print(f"✅ Glue job created: {config['aws_resources']['glue_job']['name']}")
        return response['Name']
    except glue_client.exceptions.AlreadyExistsException:
        print("⚠️  Glue job already exists")
        return config['aws_resources']['glue_job']['name']

def create_step_function(config):
    """Create Step Functions workflow"""
    sfn_client = boto3.client('stepfunctions', region_name=config['aws_resources']['region'])
    
    # Step Functions definition
    state_machine_definition = {
        "Comment": "Shopify AI Automation Workflow",
        "StartAt": "ETLJob",
        "States": {
            "ETLJob": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": config['aws_resources']['glue_job']['name']
                },
                "Next": "LambdaProcessing"
            },
            "LambdaProcessing": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": config['aws_resources']['lambda_function']['name'],
                    "Payload": {
                        "s3_bucket": config['aws_resources']['s3_bucket']['name'],
                        "parquet_key": "data/clean/clean_products.parquet"
                    }
                },
                "Next": "Success"
            },
            "Success": {
                "Type": "Succeed"
            }
        }
    }
    
    try:
        response = sfn_client.create_state_machine(
            name=config['aws_resources']['step_function']['name'],
            definition=json.dumps(state_machine_definition),
            roleArn=config['aws_resources']['iam_role']['arn']
        )
        print(f"✅ Step Function created: {response['stateMachineArn']}")
        return response['stateMachineArn']
    except sfn_client.exceptions.StateMachineAlreadyExistsException:
        print("⚠️  Step Function already exists")
        return f"arn:aws:states:{config['aws_resources']['region']}:195275669634:stateMachine:{config['aws_resources']['step_function']['name']}"

def create_s3_folders(config):
    """Create S3 folder structure"""
    s3_client = boto3.client('s3', region_name=config['aws_resources']['region'])
    bucket = config['aws_resources']['s3_bucket']['name']
    
    folders = [
        'data/raw/',
        'data/clean/',
        'scripts/',
        'lambdas/',
        'reports/',
        'temp/'
    ]
    
    for folder in folders:
        try:
            s3_client.put_object(Bucket=bucket, Key=folder)
            print(f"✅ Created S3 folder: {folder}")
        except Exception as e:
            print(f"⚠️  Error creating folder {folder}: {e}")

def main():
    """Main function to create all AWS resources"""
    print("🚀 Creating AWS Resources for Shopify AI Automation...")
    print("=" * 60)
    
    try:
        config = load_config()
        print(f"📋 Using configuration for region: {config['aws_resources']['region']}")
        
        # Create S3 folder structure
        print("\n📁 Creating S3 folder structure...")
        create_s3_folders(config)
        
        # Create Lambda function
        print("\n🔧 Creating Lambda function...")
        lambda_arn = create_lambda_function(config)
        
        # Create Glue job
        print("\n⚙️  Creating Glue ETL job...")
        glue_job_name = create_glue_job(config)
        
        # Create Step Function
        print("\n🔄 Creating Step Functions workflow...")
        sfn_arn = create_step_function(config)
        
        print("\n" + "=" * 60)
        print("🎉 AWS Resources Creation Complete!")
        print("\n📊 Summary:")
        print(f"   S3 Bucket: {config['aws_resources']['s3_bucket']['arn']}")
        print(f"   IAM Role: {config['aws_resources']['iam_role']['arn']}")
        print(f"   Lambda Function: {lambda_arn}")
        print(f"   Glue Job: {glue_job_name}")
        print(f"   Step Function: {sfn_arn}")
        
        print("\n📝 Next Steps:")
        print("   1. Upload your raw product data to s3://shopifyapi369/data/raw/")
        print("   2. Set up your .env file with API keys")
        print("   3. Test the ETL job manually")
        print("   4. Configure GitHub Actions secrets")
        
    except Exception as e:
        print(f"❌ Error creating resources: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 