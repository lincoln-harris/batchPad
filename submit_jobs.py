#////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////
# script: submit_jobs.py
# author: Jack Kamm
# date: 1.16.18
#
# can i get this working? 
#////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////
import re
import os
import json
import boto3

s3_resource = boto3.resource("s3")
batch_client = boto3.client("batch")

seq_bucket_name = "czb-seqbot"
seq_bucket_prefix = "fastqs/181214_A00111_0242_AHG5HKDSXX/rawdata/Paula_HayakawaSerpa_OPS016"

sample_names = set([])
seq_name_re = re.compile(
    r"^(OPS016_CT_Transcriptome.*)_R\d_001\.fastq\.gz$")
seq_bucket = s3_resource.Bucket(seq_bucket_name)
for obj in seq_bucket.objects.filter(Prefix=seq_bucket_prefix):
    matched = seq_name_re.match(os.path.basename(obj.key))
    if matched:
        sample_names.add(matched.group(1))

with open("logs/job_definition.json") as f:
    jobDefinition = json.load(f)["jobDefinitionArn"]

with open("logs/job_queue.json") as f:
    jobQueue = json.load(f)["jobQueueArn"]

for sample in sample_names:
    s3_fq = (f"s3://{seq_bucket_name}/{seq_bucket_prefix}/{sample}" +
             "_R{}_001.fastq.gz")
    response = batch_client.submit_job(
        jobName=sample,
        jobQueue=jobQueue,
        jobDefinition=jobDefinition,
        parameters={
            "out_prefix": "s3://jackkamm/ct-transcriptomics/gene_counts/" + sample,
            "s3_fq_1": s3_fq.format(1),
            "s3_fq_2": s3_fq.format(2),
        }
    )

    with open(f"logs/jobs/{sample}.json", "w") as f:
        json.dump(response, f, indent=4, default=str)

#////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////