"""
This module is used to perform operations on S3 (real one).
Operations can be upload files, download files, delete files, update files, etc.,
"""
import os
import boto3


class S3(object):

    @classmethod
    def upload_file_to_s3(self, file_path, bucket, key):
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(file_path, bucket, key)

    @classmethod
    def download_from_s3(self, bucket, key, filename):
        s3.meta.client.download_file(bucket, key, filename)


class SparkJob(object):

    def __init__(self, input_url, job_flow_id=None):
        self.job_flow_id = 'j-2JN6C5J4PF4Z1'
        self.input_url = input_url
        self.file_name = input_url.split('/')[-1]
        if '.' in self.file_name:
            self.file_name = self.file_name.split('.')[0]
        if job_flow_id:
            self.job_flow_id = job_flow_id

    def setUp(self, code):
        py_file = self.file_name + ".py"
        f = open(py_file, 'w')
        f1 = open('main.py', 'r')
        f1_data = f1.read()
        code += '\n\n' + f1_data
        f.write(code)
        f.close()
        S3.upload_file_to_s3(py_file, "dianemo", "scripts/"+py_file)
        self.script_path = "s3://dianemo/scripts/" + py_file

    def run_spark_job(self):
        client = boto3.client('emr')
        input_url = self.input_url
        output_url = 's3://dianemo/output/' + self.file_name + '.txt'
        local_output_path = '/home/hadoop/' + self.file_name + "-output.txt"
        steps = [
            {
                "Name": "Copying script",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": ["aws", "s3", "cp", self.script_path, '/home/hadoop/script.py']
                }
            },
            {
                "Name": "Running new job",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": ['/usr/bin/spark-submit', '/home/hadoop/script.py', input_url]
                }
            },
            {
                "Name": "Uploading output to S3",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": ["aws", "s3", "cp", local_output_path, output_url]
                }
            }
        ]
        # steps_new = [
        #     {
        #         "Name": "Running new job",
        #         "ActionOnFailure": "CANCEL_AND_WAIT",
        #         "HadoopJarStep": {
        #             "Jar": "command-runner.jar",
        #             "Args": ['/usr/bin/spark-submit', '/home/hadoop/wordcount.py', 's3n://he-bigdata/request_log/2015/10/12/*.json']
        #         }
        #     },
        # ]
        action = client.add_job_flow_steps(
                    JobFlowId=self.job_flow_id,
                    Steps=steps)
        print("Added step: %s"%(action))
