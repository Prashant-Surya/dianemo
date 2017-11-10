import boto3, os

class Distributor(object):

    def __init__(self, input_url, output_url):
        """
        Input and output will be S3 urls. Starting with S3:/// file format.
        Output file shouldn't be there already. We'll be using this file for
        checking if the job is executed.
        """
        self.input_url = input_url
        self.output_url = output_url

    def job(self, input):
        """
        :param input: passes the input_url to be used in the code to perform
        operations on it.
        :returns output: as string which we'll upload to S3.
        """
        raise NotImplementedError

    def start_job(self):
        """
        Takes care of actual work of running the job.
        """
        output = self.job(self.input_url)
        #TODO: upload output to s3 to the output_url
