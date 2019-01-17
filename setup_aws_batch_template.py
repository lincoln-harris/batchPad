#////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////
# script: setup_aws_batch.py
# author: Jack Kamm
# date: 1.16.18
#
# can i get this working? 
#////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////

import time
import json
import base64
import boto3

ec2_client = boto3.client("ec2")
batch_client = boto3.client("batch")

ec2KeyPair = "lincoln-harris"

launch_template = "lincoln-test-template"
compute_environment = "lincoln-test-environment"
job_queue = "lincoln-test-queue"
job_definition = "lincoln-test-job-def"

root_volume_size = 2000
#instance_types = ["m4", "r4"]
instance_types = ["optimal"]

job_image = 'broadinstitute/gatk'
job_vcpus = 16
job_memory = 64
job_cmd = [
    "count_genes.py", "Ref::out_prefix",
    "Ref::s3_fq_1", "Ref::s3_fq_2",
    "--threads", str(job_vcpus)
]

user_data = """MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="
--==MYBOUNDARY==
Content-Type: text/x-shellscript; charset="us-ascii"
#!/bin/bash
mkdir -p /scratch
echo '* soft nofile 1000000' >> /etc/security/limits.d/20-nfile.conf
echo '* hard nofile 1000000' >> /etc/security/limits.d/20-nfile.conf
yum install -y aws-cli
aws s3 sync s3://jackkamm/ct-transcriptomics/ /scratch/ --exclude '*' \
    --include 'star_genomeGenerate_grch38*' \
    --include 'GCF_000068585.1_ASM6858v1_genomic.fna*' \
    --include GCF_000068585.1_ASM6858v1_genomic.gff
--==MYBOUNDARY==--
"""

if True:
    print("Creating launch template...")
    with open("logs/launch_template.json", "w") as f:
        json.dump(
            ec2_client.create_launch_template(
                LaunchTemplateName=launch_template,
                LaunchTemplateData={
                    "BlockDeviceMappings": [
                        {
                            "DeviceName": "/dev/xvda",
                            "Ebs": {
                                "DeleteOnTermination": True,
                                "VolumeSize": root_volume_size,
                                "VolumeType": "gp2"
                            }
                        }
                    ],
                    'UserData': base64.b64encode(
                        user_data.encode("ascii")).decode("ascii"),
                }
            ),
            f, default=str, indent=4
        )
    print("Finished creating launch template.")

    print("Creating compute environment...")
    with open("logs/compute_environment.json", "w") as f:
        compute_resources = {
            'type': 'SPOT',
            'minvCpus': 0,
            'maxvCpus': 256,
            'instanceTypes': instance_types,
            'subnets': [
                # subnets for us-west-2a, us-west-2b, us-west-2c
                "subnet-672e832e",
                "subnet-04119a63",
                "subnet-4347451b",
            ],
            'securityGroupIds': [
                'sg-3195a049',
            ],
            "ec2KeyPair": ec2KeyPair,
            'instanceRole': 'ecsInstanceRole2',
            'bidPercentage': 100,
            'spotIamFleetRole': 'arn:aws:iam::423543210473:role/aws-ec2-spot-fleet-role',
            'launchTemplate': {
                'launchTemplateName': launch_template
            }
        }
        json.dump(
            batch_client.create_compute_environment(
                computeEnvironmentName=compute_environment,
                type='MANAGED',
                state='ENABLED',
                computeResources=compute_resources,
                serviceRole='arn:aws:iam::423543210473:role/AWSBatchServiceRole'
            ),
            f, default=str, indent=4
        )
    print("Finished creating compute environment.")

    print("Creating job queue...")
    n_tries = 5
    sleep_time = 30
    for i in range(n_tries):
        desc, = batch_client.describe_compute_environments(
            computeEnvironments=[compute_environment]
        )["computeEnvironments"]
        if desc['status'] != 'VALID':
            print("Waiting for compute environment...",
                  f"(Try {i+1}/{n_tries})")
            time.sleep(sleep_time)
        else:
            break
    with open("logs/job_queue.json", "w") as f:
        json.dump(
            batch_client.create_job_queue(
                jobQueueName=job_queue,
                state='ENABLED',
                priority=5,
                computeEnvironmentOrder=[
                    {
                        'order': 5,
                        'computeEnvironment': compute_environment
                    },
                ]
            ),
            f, default=str, indent=4
        )
    print("Finished creating job queue.")

    print("Creating job definition...")
    with open("logs/job_definition.json", "w") as f:
        json.dump(
            batch_client.register_job_definition(
                jobDefinitionName=job_definition,
                type='container',
                containerProperties={
                    'image': job_image,
                    'vcpus': job_vcpus,
                    'memory': job_memory,
                    'command': job_cmd,
                    "volumes": [
                        {"host": {"sourcePath": "/scratch"},
                         "name": "scratch"},
                    ],
                    "mountPoints": [
                        {"containerPath": "/scratch",
                         "sourceVolume": "scratch"},
                    ],
                    'jobRoleArn': 'arn:aws:iam::423543210473:role/simpleBatchJob',
                    "privileged": True
                },
            ),
            f, default=str, indent=4
        )
    print("Finished creating job definition.")

#////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////