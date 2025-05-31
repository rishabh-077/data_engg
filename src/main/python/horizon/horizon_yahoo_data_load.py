"""
DAG Name: lmdh_horizon_gold_martech_bq_lormn_offsite_yahoo_full_daily

Description:
This DAG extracts yahoo data and creates views namely line id level,gender level,age level,product sku level data from MarTech source tables,processes it, and loads the data into our gold tables in GCP BigQuery.
From where we run the connexio pipeline to finally load the data into LORMN Team's GCS buckets for consumption of Horizon Team.

Source Tables
daci_media_mrkt_silver.martech_yahoo_conversion_metrics
daci_media_mrkt_silver.martech_yahoo_engagement_metrics
daci_media_mrkt_silver.martech_yahoo_product_info_metrics
daci_media_mrkt_silver.yahoo_campaign_data

Target Tables
daci_lmdh_gold.horizon_yahoo_product_sku
daci_lmdh_gold.horizon_yahoo_line
daci_lmdh_gold.horizon_yahoo_age
daci_lmdh_gold.horizon_yahoo_gender
daci_lmdh_gold.horizon_yahoo_meta_data
daci_lmdh_gold.horizon_yahoo_camp_discrepancy

Schedule:
Runs on daily basis as source tables frequency is daily
"""

import argparse
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import pandas as pd
from gcp_common_functions import GCPCommonFunctions
from spark_common_functions import SparkCommonFunctions
from python_common_functions import PythonCommonFunctions


class InputParameter(object):
    """
     To get all the input parameters
     and read it
    """

    def __init__(self):
        self.yaml_path = ""
        self.bq_project = ""
        self.gold_dataset = ""
        self.raw_dataset = ""
        self.mrkt_project = ""
        self.mrkt_dataset = ""
        self.start_date = ""
        self.end_date = ""
        self.temp_dataset = ""
        self.temp_bucket = ""
        self.query_list = ""

    # Get all the input parameters..
    def get_input_parameters(self):
        self.my_parser = argparse.ArgumentParser()
        self.my_parser.add_argument(
            '--yaml_path', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--bq_project', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--gold_dataset', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--raw_dataset', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--mrkt_project', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--mrkt_dataset', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--start_date', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--end_date', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--temp_dataset', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--temp_bucket', action='store', type=str, required=False)
        self.my_parser.add_argument(
            '--query_list', action='store', type=str, required=False)

    # Read the input parameters:
    def read_input_parameters(self):
        input_args = self.my_parser.parse_args()
        self.yaml_path = input_args.yaml_path
        self.bq_project = input_args.bq_project
        self.gold_dataset = input_args.gold_dataset
        self.raw_dataset = input_args.raw_dataset
        self.mrkt_project = input_args.mrkt_project
        self.mrkt_dataset = input_args.mrkt_dataset
        self.start_date = input_args.start_date
        self.end_date = input_args.end_date
        self.temp_dataset = input_args.temp_dataset
        self.temp_bucket = input_args.temp_bucket
        self.query_list = input_args.query_list

    def get_all_parameters(self):
        self.get_input_parameters()
        self.read_input_parameters()

class GCPHorizonYahooLoad(object):
    """
    To run the queries through GCP
    fetch and write the data to target gcp tables
    """

    def __init__(self) -> None:
        self.param = InputParameter()
        self.param.get_all_parameters()
        self.gcf = GCPCommonFunctions()
        self.scf = SparkCommonFunctions()
        self.pcf = PythonCommonFunctions()
        self.spark = self.scf.get_spark_session(
            'yahoo_data_load',
            materialization_dataset=self.param.temp_dataset,
            temporary_gcs_bucket=self.param.temp_bucket
        )

    def run_gbq_query(self):
        # running the query in BQ
        config_data = self.pcf.read_yaml(self.param.yaml_path)
        print(f"The config data: {config_data}")
        gcp_queries = config_data['gcp_queries']
        print("YAML Config has been read")

        query_names = [query_name.strip() for query_name in self.param.query_list.split(",")]
        print(f"The Query Names List: {query_names}")

        # Filter gcp_queries based on query_names
        selected_queries = [query for query in gcp_queries if query['name'].strip() in query_names]
        print(f"The Selected Queries List: {selected_queries}")

        for query in selected_queries:

            query_name = query['name']
            pre_bq_query = query['target_bq_query']
            bq_table = query['target_bq_table']
            write_mode = query['bq_write_mode']

            print("query name is:" + query_name)

            print("calculating start date and end date if not given")
            if self.param.start_date == "NA":
                print("calculating max date from target table")
                max_dt_qry = config_data['max_target_dt_query'].format(self.param.gold_dataset,
                                                                       bq_table)
                print(max_dt_qry)
                start_date = self.scf.spark_read_bq(self.spark, query=max_dt_qry).first()[0]
            else:
                start_date = datetime.strptime(self.param.start_date, '%Y-%m-%d').date()
            print('Start Date is :', start_date)

            if self.param.end_date == "NA":
                print("calculating max date from source table")
                end_date_lst = []
                for src_dt_qry in query['max_src_dt_queries']:
                    tbl_end_date = self.gcf.read_bq(src_dt_qry.format(self.param.mrkt_project,
                                                                      self.param.mrkt_dataset)
                                                    ).iloc[0, 0]
                    end_date_lst.append(tbl_end_date)
                end_date = min(end_date_lst) + timedelta(days=1)
                print(f"common end date for table {bq_table} is (end_date + 1): {str(end_date)}")
            else:
                end_date = datetime.strptime(self.param.end_date, '%Y-%m-%d').date()
                print('End Date is :', end_date)

            if start_date + timedelta(days=1) >= end_date:
                print("(Start date + 1 day) is equal to or greater than end date. "
                      "Data has already been loaded into target table for available partitions")
                continue

            else:
                if query_name == "Q_horizon_yahoo_meta_data_query":
                    print(f"Executing query: {query_name}")
                    bq_query = pre_bq_query.format(
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        start_date,
                        end_date,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        start_date,
                        end_date,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        start_date,
                        end_date,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        start_date,
                        end_date,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                    )

                elif query_name == "Q_horizon_yahoo_product_sku_query":
                    print(f"Executing query: {query_name}")
                    bq_query = pre_bq_query.format(
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        start_date,
                        end_date,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        self.param.bq_project,
                        self.param.gold_dataset
                    )

                elif query_name == "Q_horizon_yahoo_camp_hist_query":
                    print(f"Executing query: {query_name}")
                    bq_query = pre_bq_query.format(
                        self.param.bq_project,
                        self.param.gold_dataset,
                        self.param.bq_project,
                        self.param.gold_dataset
                    )

                elif query_name == "Q_horizon_yahoo_discrepancy_query":
                    print(f"Executing query: {query_name}")
                    bq_query = pre_bq_query.format(
                        self.param.bq_project,
                        self.param.gold_dataset
                    )

                else:
                    print(f"Executing query: {query_name}")
                    bq_query = pre_bq_query.format(
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        start_date,
                        end_date,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        start_date,
                        end_date,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        start_date,
                        end_date,
                        self.param.mrkt_project,
                        self.param.mrkt_dataset,
                        start_date,
                        end_date,
                        self.param.bq_project,
                        self.param.gold_dataset
                    )

                print("***************************")
                print("The bq_query:" + bq_query)
                print("bq_project is:" + self.param.bq_project)
                print("gold_dataset is:" + self.param.gold_dataset)
                print("raw_dataset is:" + self.param.raw_dataset)
                print("source_project is:" + self.param.mrkt_project)
                print("source_dataset is:" + self.param.mrkt_dataset)
                print("bq_table is:" + bq_table)
                print("write_mode is:" + write_mode)

                # inserting new data
                raw_data_df = self.gcf.read_bq(bq_query)
                table_id = f"{self.param.bq_project}.{self.param.raw_dataset}.{bq_table}"

                print(raw_data_df.head())
                print(raw_data_df.dtypes)

                # imposing schema for loading data to big query
                schema = query.get('schema', {})
                if schema:
                    print("Enforcing schema before writing to BigQuery")
                    for col_name, col_type in schema.items():
                        if col_name in raw_data_df.columns:
                            try:
                                if col_type == "bignumeric":
                                    raw_data_df[col_name] = raw_data_df[col_name].apply(lambda x: Decimal(x) if x is not None else None)
                                elif col_type == "bignum":
                                    def safe_decimal(value):
                                        if value is None:
                                            return None
                                        try:
                                            # Rescale to BigQuery limits (38 precision, 9 scale)
                                            return Decimal(value).quantize(Decimal('1e-9'))
                                        except (InvalidOperation, ValueError) as e:
                                            print(f"Value '{value}' in column '{col_name}' caused error: {e}")
                                            return None
                                    raw_data_df[col_name] = raw_data_df[col_name].apply(safe_decimal)
                                elif col_type == "date":
                                    raw_data_df[col_name] = pd.to_datetime(raw_data_df[col_name], errors='coerce').dt.date
                                else:
                                    raw_data_df[col_name] = raw_data_df[col_name].astype(col_type)
                            except Exception as e:
                                print(f"Error casting column {col_name} to type {col_type}: {e}")
                                raise

                print(raw_data_df.head())
                print(raw_data_df.dtypes)

                print("writing data into bigquery")
                self.gcf.write_bq(raw_data_df,
                                  self.param.bq_project,
                                  self.param.raw_dataset,
                                  bq_table,
                                  write_mode)

                print(f"data has been successfully inserted into table {table_id}")

                pre_upsert_bq_query = query['upsert_bq_query']
                upsert_bq_table = query['upsert_bq_table']

                upsert_bq_query = pre_upsert_bq_query.format(
                    self.param.gold_dataset,
                    self.param.raw_dataset
                )

                print("The upsert_bq_query:" + upsert_bq_query)
                print("The upsert_bq_table:" + upsert_bq_table)

                upsert_table_id = f"{self.param.bq_project}.{self.param.gold_dataset}.{upsert_bq_table}"

                self.gcf.update_bq(upsert_bq_query)

                print(f"data has been successfully inserted into table {upsert_table_id}")

                print("***************************")

    def main_procedure(self):
        self.run_gbq_query()


if __name__ == "__main__":
    process = GCPHorizonYahooLoad()
    process.main_procedure()
