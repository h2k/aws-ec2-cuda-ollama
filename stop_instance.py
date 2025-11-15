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

#############################################
# ec2 = boto3.client("ec2")

# def lambda_handler(event, context):
#     instance_id = os.environ.get("INSTANCE_ID")

#     if not instance_id:
#         return {"status": "error", "message": "INSTANCE_ID not set in environment variables"}

#     try:
#         ec2.stop_instances(InstanceIds=[instance_id])
#         return {"status": "success", "action": "stop", "instance_id": instance_id}

#     except Exception as e:
#         return {"status": "error", "message": str(e)}

##############################################################


import boto3
import os
from datetime import datetime, timedelta, timezone

ec2 = boto3.client("ec2")
cloudwatch = boto3.client("cloudwatch")

def is_instance_idle(instance_id, threshold=5, duration_minutes=60):
    """
    Check CPUUtilization for the last 'duration_minutes'.
    Return True if average CPU is below threshold.
    """

    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=duration_minutes)

    response = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "cpu_usage",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EC2",
                        "MetricName": "CPUUtilization",
                        "Dimensions": [
                            {"Name": "InstanceId", "Value": instance_id}
                        ],
                    },
                    "Period": 300,  # 5 minutes
                    "Stat": "Average",
                },
                "ReturnData": True,
            }
        ],
        StartTime=start,
        EndTime=end,
    )

    values = response["MetricDataResults"][0]["Values"]

    if not values:
        # No data — treat as idle OR return False based on your policy
        return True

    avg_cpu = sum(values) / len(values)

    print(f"Average CPU for last {duration_minutes} minutes = {avg_cpu}%")

    return avg_cpu < threshold


def lambda_handler(event, context):
    instance_id = os.environ.get("INSTANCE_ID")

    if not instance_id:
        return {"status": "error", "message": "INSTANCE_ID not set"}

    try:
        # 1️⃣ Check if idle
        if not is_instance_idle(instance_id):
            return {
                "status": "not_idle",
                "message": f"{instance_id} is not idle. Will not stop."
            }

        # 2️⃣ Stop EC2
        ec2.stop_instances(InstanceIds=[instance_id])

        return {
            "status": "success",
            "action": "stop",
            "instance_id": instance_id,
            "reason": "Instance was idle for 1 hour"
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}
