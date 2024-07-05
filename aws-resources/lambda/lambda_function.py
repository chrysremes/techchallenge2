from __future__ import print_function
import boto3
import urllib

print('Loading function')

glue = boto3.client('glue')

def lambda_handler(event, context):
    gluejobname="First_Job"
    print(f'Loading gluejobname={gluejobname}')
    try:
        print('trying')
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])
    except Exception as e:
        print('exception!')
        print(e)