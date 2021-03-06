import os, sys

from dianemo.aws import S3

class Distributor(object):

    def __init__(self, input_url):
        """
        Input and output will be S3 urls. Starting with S3:/// file format.
        Output file shouldn't be there already. We'll be using this file for
        checking if the job is executed.
        """
        self.input_url = input_url
        #self.output_url = output_url

    def start_job(self):
        """
        Takes care of actual work of running the job.
        Write a job method in the script.py file which will be imported and used
        here for calling the actual implementation.
        It may be improved but we'll see that later.
        """

        output = job(self.input_url)
        file_name = self.input_url.split('/')[-1].split('.')[0]
        output_name = file_name + "-output.txt"
        local_output_path = '/home/hadoop/' + output_name
        f = open(local_output_path, 'w')
        f.write(output)
        f.close()
        print("Moving from " + local_output_path + " to " + 'dianemo/' + output_name)
        S3.upload_file_to_s3(local_output_path, 'dianemo', output_name)


if __name__ == '__main__':
    input_url = sys.argv[1]
    d = Distributor(input_url)
    d.start_job()
