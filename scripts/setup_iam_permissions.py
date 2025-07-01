#!/usr/bin/env python3
"""
IAM Permissions Setup Script for Shopify AI Automation
Sets up required permissions for Lambda, Glue, and Step Functions
"""

import boto3
import json
from pathlib import Path

def load_config():
    """Load AWS resource configuration"""
    config_path = Path(__file__).parent.parent / "config" / "aws_resources.json"
    with open(config_path, 'r') as f:
        return json.load(f)

def create_lambda_execution_policy(config):
    """Create policy for Lambda execution"""
    iam_client = boto3.client('iam', region_name=config['aws_resources']['region'])
    
    policy_name = "ShopifyLambdaExecutionPolicy"
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    config['aws_resources']['s3_bucket']['arn'],
                    f"{config['aws_resources']['s3_bucket']['arn']}/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "lambda:InvokeFunction"
                ],
                "Resource": "*"
            }
        ]
    }
    
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description="Policy for Shopify Lambda function execution"
        )
        print(f"✅ Created Lambda execution policy: {response['Policy']['Arn']}")
        return response['Policy']['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        print("⚠️  Lambda execution policy already exists")
        return f"arn:aws:iam::195275669634:policy/{policy_name}"

def create_glue_execution_policy(config):
    """Create policy for Glue execution"""
    iam_client = boto3.client('iam', region_name=config['aws_resources']['region'])
    
    policy_name = "ShopifyGlueExecutionPolicy"
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    config['aws_resources']['s3_bucket']['arn'],
                    f"{config['aws_resources']['s3_bucket']['arn']}/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:BatchCreatePartition",
                    "glue:BatchDeletePartition",
                    "glue:BatchGetPartition"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }
    
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description="Policy for Shopify Glue job execution"
        )
        print(f"✅ Created Glue execution policy: {response['Policy']['Arn']}")
        return response['Policy']['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        print("⚠️  Glue execution policy already exists")
        return f"arn:aws:iam::195275669634:policy/{policy_name}"

def create_step_functions_policy(config):
    """Create policy for Step Functions execution"""
    iam_client = boto3.client('iam', region_name=config['aws_resources']['region'])
    
    policy_name = "ShopifyStepFunctionsPolicy"
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "lambda:InvokeFunction"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "iam:PassRole"
                ],
                "Resource": config['aws_resources']['iam_role']['arn']
            }
        ]
    }
    
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description="Policy for Shopify Step Functions execution"
        )
        print(f"✅ Created Step Functions policy: {response['Policy']['Arn']}")
        return response['Policy']['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        print("⚠️  Step Functions policy already exists")
        return f"arn:aws:iam::195275669634:policy/{policy_name}"

def attach_policies_to_role(config, policy_arns):
    """Attach policies to the IAM role"""
    iam_client = boto3.client('iam', region_name=config['aws_resources']['region'])
    role_name = config['aws_resources']['iam_role']['name']
    
    for policy_arn in policy_arns:
        try:
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
            print(f"✅ Attached policy {policy_arn} to role {role_name}")
        except iam_client.exceptions.EntityAlreadyExistsException:
            print(f"⚠️  Policy {policy_arn} already attached to role {role_name}")

def create_trust_policy(config):
    """Create trust policy for the role"""
    iam_client = boto3.client('iam', region_name=config['aws_resources']['region'])
    role_name = config['aws_resources']['iam_role']['name']
    
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com",
                        "glue.amazonaws.com",
                        "states.amazonaws.com"
                    ]
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        iam_client.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=json.dumps(trust_policy)
        )
        print(f"✅ Updated trust policy for role {role_name}")
    except Exception as e:
        print(f"⚠️  Error updating trust policy: {e}")

def main():
    """Main function to set up IAM permissions"""
    print("🔐 Setting up IAM Permissions for Shopify AI Automation...")
    print("=" * 60)
    
    try:
        config = load_config()
        print(f"📋 Using configuration for region: {config['aws_resources']['region']}")
        
        # Create policies
        print("\n📜 Creating IAM policies...")
        lambda_policy_arn = create_lambda_execution_policy(config)
        glue_policy_arn = create_glue_execution_policy(config)
        sfn_policy_arn = create_step_functions_policy(config)
        
        # Update trust policy
        print("\n🤝 Updating role trust policy...")
        create_trust_policy(config)
        
        # Attach policies to role
        print("\n🔗 Attaching policies to role...")
        policy_arns = [lambda_policy_arn, glue_policy_arn, sfn_policy_arn]
        attach_policies_to_role(config, policy_arns)
        
        print("\n" + "=" * 60)
        print("🎉 IAM Permissions Setup Complete!")
        print("\n📊 Summary:")
        print(f"   Role: {config['aws_resources']['iam_role']['arn']}")
        print(f"   Lambda Policy: {lambda_policy_arn}")
        print(f"   Glue Policy: {glue_policy_arn}")
        print(f"   Step Functions Policy: {sfn_policy_arn}")
        
        print("\n📝 Next Steps:")
        print("   1. Run the AWS resource creation script")
        print("   2. Test the Lambda function")
        print("   3. Test the Glue job")
        print("   4. Set up GitHub Actions secrets")
        
    except Exception as e:
        print(f"❌ Error setting up IAM permissions: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 