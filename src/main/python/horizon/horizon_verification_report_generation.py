"""
This script reads configuration parameters from a YAML file and command-line arguments,
executes dynamic BigQuery queries using Spark, formats the results into HTML,
and uploads the output to Google Cloud Storage (GCS).
"""

import argparse
import yaml
import time
import os
import io
from google.cloud import storage
from datetime import datetime, timedelta
from ast import literal_eval
from gcp_common_functions import GCPCommonFunctions
from spark_common_functions import SparkCommonFunctions
from python_common_functions import PythonCommonFunctions


class InputParameter(object):
    """
         Class to handle and parse input parameters required for the horizon data verification process.
         """

    def __init__(self):
        self.my_parser = None
        self.yaml_path = ""
        self.start_dt = ""
        self.end_dt = ""
        self.report_dt = ""
        self.temp_dataset = ""
        self.temp_bucket = ""
        self.output_file_name = ""
        self.bq_project = ""
        self.mrkt_project = ""

    # Get all the input parameters..
    def get_input_parameters(self):
        self.my_parser = argparse.ArgumentParser()
        self.my_parser.add_argument(
            '--yaml_path', action='store', type=str, required=True)
        self.my_parser.add_argument('--start_dt', action='store', type=str, required=True,help='start date')
        self.my_parser.add_argument('--end_dt', action='store', type=str, required=True,help='end date')
        self.my_parser.add_argument('--report_dt', action='store', type=str, required=True,help='report date')
        self.my_parser.add_argument(
            '--temp_dataset', action='store', type=str, required=True)
        self.my_parser.add_argument(
            '--temp_bucket', action='store', type=str, required=True)
        self.my_parser.add_argument(
            '--output_file_name', action='store', type=str, required=True)
        self.my_parser.add_argument(
            '--bq_project', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--mrkt_project', action='store', type=str, required=False)

    # Read the input parameters:
    def read_input_parameters(self):
        input_args = self.my_parser.parse_args()
        self.yaml_path = input_args.yaml_path
        self.start_dt = input_args.start_dt
        self.end_dt = input_args.end_dt
        self.report_dt = input_args.report_dt
        self.temp_dataset = input_args.temp_dataset
        self.temp_bucket = input_args.temp_bucket
        self.output_file_name = input_args.output_file_name
        self.bq_project = input_args.bq_project
        self.mrkt_project = input_args.mrkt_project

    def get_all_parameters(self):
        self.get_input_parameters()
        self.read_input_parameters()


class TransferData(object):
    """
    Class to manage the generation of metric verification reports.
    It handles reading configurations, querying BigQuery via Spark, formatting results into HTML,
    and writing the final report to Google Cloud Storage (GCS).
    """
    def __init__(self) -> None:
        self.param = InputParameter()
        self.param.get_all_parameters()
        self.gcf = GCPCommonFunctions()
        self.scf = SparkCommonFunctions()
        self.pcf = PythonCommonFunctions()
        self.spark = self.scf.get_spark_session(
            'horizon_data_verification',
            materialization_dataset=self.param.temp_dataset,
            temporary_gcs_bucket=self.param.temp_bucket
        )

    @staticmethod
    def yaml_load(input_path):
            print("loading Yaml File")
            with open(input_path) as file:
                yaml_configs = yaml.safe_load(file)
                return yaml_configs

    def calculate_date(self):
        curr_dt = datetime.today()
        print('Input datetime:', curr_dt.date())

        self.param.end_dt = curr_dt - timedelta(days=2)
        self.param.end_dt = self.param.end_dt.date()
        print('End Date is: ', self.param.end_dt)

        self.param.start_dt = curr_dt - timedelta(days=13)
        self.param.start_dt = self.param.start_dt.date()
        print('Start Date is :', self.param.start_dt)

        self.param.report_dt = curr_dt - timedelta(days=1)
        self.param.report_dt = self.param.report_dt.date()
        print('Report Date is: ', self.param.report_dt)


    def execute_query(self, query):
        try:
            query_name = query['name']

            if query_name == 'Q_horizon_yahoo_data_quality_check':
                bq_query = query['target_bq_query'].format(
                    self.param.bq_project,
                    self.param.mrkt_project,
                    self.param.mrkt_project,
                    self.param.bq_project,
                    self.param.mrkt_project,
                    self.param.mrkt_project,
                    self.param.bq_project,
                    self.param.mrkt_project,
                    self.param.mrkt_project,
                    self.param.bq_project,
                    self.param.mrkt_project
                )

            elif query_name == 'Q_horizon_yahoo_data_check':
                bq_query = query['target_bq_query'].format(
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt)
                )

            elif query_name == 'Q_horizon_meta_data_check':
                bq_query = query['target_bq_query'].format(
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt),
                    self.param.bq_project,
                    str(self.param.report_dt)
                )

            else:
                bq_query = query['target_bq_query'].format(str(self.param.start_dt),
                                                       str(self.param.end_dt),
                                                       str(self.param.start_dt),
                                                       str(self.param.end_dt))

            # Execute the BQ query using Spark SQL
            print(f"Executing BQ query: {bq_query}")
            target_df = self.scf.spark_read_bq(self.spark, query=bq_query)

            # Write DataFrame to Hadoop table in append mode
            try:
                target_df.show(10)
                pandas_df = target_df.toPandas()

                formatted_report_content = pandas_df.to_html(index=False)
                print(f"Data has been successfully created in HTML format")
                return formatted_report_content

            except Exception as e:
                raise (f"Error occurred while creating HTML format data: {str(e)}")
        except Exception as e:
            raise (f"Error occurred while creating HTML format data: {str(e)}")

    @staticmethod
    def write_gcs_file(bucket_name, file_name, data):
        """
        Write a pandas DataFrame in a text/html format to a file in Google Cloud Storage (GCS).

        Args:
            bucket_name (str): The name of the GCS bucket.
            prefix (str): The prefix path in the bucket.
            file_name (str): The name of the file to write.
            data (pd.DataFrame): The DataFrame to write to GCS.
        """
        print(f"Writing file {file_name} to bucket {bucket_name}")
        bucket = storage.Client().bucket(bucket_name)
        blob = bucket.blob(file_name)

        with blob.open("w") as f:
            f.write(data)
        print("File written successfully")

    def main_procedure(self):
        config_data = self.yaml_load(self.param.yaml_path)
        print(f"The config Data: {config_data}")
        if not config_data:
            return

        bq_queries = config_data['bq_queries']

        if self.param.start_dt == "NA" and self.param.end_dt == "NA" and self.param.report_dt == "NA":
            self.calculate_date()

        email_content = f""" """
        for query in bq_queries:
            html_format = self.execute_query(query)
            email_content = email_content + "<p><b><u>{}</u></b></p>".format(query['email_desc'])
            email_content = email_content + "<div>{}</div>".format(html_format)
            email_content = email_content + "<div> </div>"

        self.write_gcs_file(self.param.temp_bucket, self.param.output_file_name, email_content)


if __name__ == "__main__":
    loadExecution = TransferData()
    loadExecution.main_procedure()