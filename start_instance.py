import boto3
import os

# 🔐 IAM Role Policy Needed for Both
# Attach this policy to each Lambda role:
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "Action": [
#         "ec2:StartInstances",
#         "ec2:StopInstances",
#         "ec2:DescribeInstances"
#       ],
#       "Resource": "*"
#     }
#   ]
# }


ec2 = boto3.client("ec2")

def lambda_handler(event, context):
    instance_id = os.environ.get("INSTANCE_ID")

    if not instance_id:
        return {"status": "error", "message": "INSTANCE_ID not set in environment variables"}

    try:
        ec2.start_instances(InstanceIds=[instance_id])
        return {"status": "success", "action": "start", "instance_id": instance_id}

    except Exception as e:
        return {"status": "error", "message": str(e)}
