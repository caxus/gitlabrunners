
import subprocess
import sys

subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'sagemaker==2.69.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'fsspec==2021.11.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 's3fs==2021.11.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'awswrangler==2.14.0'])

import argparse
import boto3
import csv
import json
import logging
import os
import pathlib
import re
import sagemaker
import time
import pandas as pd
import awswrangler as wr
from datetime import date, datetime, timedelta
from sagemaker.feature_store.feature_group import FeatureGroup
from typing import Tuple, Union, List

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# get clients
s3 = boto3.client("s3", region_name='us-east-1')
sm = boto3.client("sagemaker", region_name='us-east-1')
featurestore_client = boto3.client('sagemaker-featurestore-runtime',
                                   region_name='us-east-1')

# get session
boto_session = boto3.session.Session(region_name='us-east-1')
sagemaker_session = sagemaker.Session(boto_session=boto_session)
role = sagemaker.get_execution_role(sagemaker_session=sagemaker_session)

def check_feature_group_status(feature_group):
    status = feature_group.describe().get("FeatureGroupStatus")
    while status == "Creating":
        print("Waiting for Feature Group to be Created")
        time.sleep(5)
        status = feature_group.describe().get("FeatureGroupStatus")
    print(f"FeatureGroup {feature_group.name} successfully created.")


def check_feature_group(
    dataframe: pd.DataFrame,
    feature_group_name: str,
    record_identifier_name: str,
    time_identifier_name: str,
    bucket_name: str,
    prefix_name: str,
    enable_online_store: bool
):
    logger.info("Started CHECK FEATURE GROUP process for {}".format(
        feature_group_name))

    # instantiate feature group
    feature_group = FeatureGroup(name=feature_group_name,
                                 sagemaker_session=sagemaker_session)

    # load feature definitions
    feature_group.load_feature_definitions(data_frame=dataframe)

    # get feature group status
    try:
        # describe feature group status
        fg_status = feature_group.describe().get("FeatureGroupStatus")
    except:
        logger.info("Feature Group NOT FOUND {}".format(feature_group_name))
        fg_status = 'NotCreated'

    # create feature group if it does not exist
    if fg_status == 'NotCreated':
        logger.info("CREATING Feature Group {}".format(feature_group_name))
        
        s3_uri = f"s3://{bucket_name}/{prefix_name}"
        print('s3uri: {}'.format(s3_uri))
        logger.info("s3uri: {}".format(s3_uri))
        # create feature group
        feature_group.create(
            s3_uri=s3_uri,
            record_identifier_name=record_identifier_name,
            event_time_feature_name=time_identifier_name,
            role_arn=role,
            enable_online_store=enable_online_store)

        # check feature group status
        check_feature_group_status(feature_group)
        return (feature_group,
                feature_group.describe().get("FeatureGroupStatus"))

    elif fg_status == 'Created':
        print(f"FeatureGroup {feature_group.name} already created.")
        return feature_group, fg_status

    else:
        print(f"FeatureGroup {feature_group.name} with status {fg_status}.")
        return feature_group, fg_status


def cast_dataframe(dataframe):
    for label in dataframe.columns:
        if dataframe.dtypes[label] == "object":
            dataframe[label] = dataframe[label].astype("str").astype("string")
        elif dataframe.dtypes[label] in ["int8", "int16", "int32", pd.Int8Dtype(), pd.Int16Dtype(), pd.Int32Dtype()]:
            dataframe[label] = dataframe[label].astype(pd.Int64Dtype())
            dataframe[label] = dataframe[label].fillna(-1)
            dataframe[label] = dataframe[label].astype(int)
        elif dataframe.dtypes[label] == pd.Int64Dtype():
            dataframe[label] = dataframe[label].fillna(-1)
            dataframe[label] = dataframe[label].astype(int)
        elif dataframe.dtypes[label] in ["float16", "float32"]:
            dataframe[label] = dataframe[label].astype(float)


if __name__ == "__main__":
    logger.info("Starting transformation.")

    # ---- create argparse and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_bucket", type=str)
    parser.add_argument("--input_prefix", type=str)
    parser.add_argument("--bucket_name_feature_store", type=str)
    parser.add_argument("--prefix_name_feature_store", type=str)
    parser.add_argument("--feature_group_name", type=str)
    parser.add_argument("--max_processes", type=str)
    parser.add_argument("--max_workers", type=str)
    args = parser.parse_args()

    # retrieve argument info
    logger.info("Received arguments {}".format(args))
    bucket_name = args.input_bucket
    prefix_name = args.input_prefix
    bucket_name_feature_store = args.bucket_name_feature_store
    prefix_name_feature_store = args.prefix_name_feature_store
    feature_group_name = args.feature_group_name
    max_processes = int(args.max_processes)
    max_workers = int(args.max_workers)

    # ----- read data
    logger.info("Reading data.")
    fullpath = "s3://" + os.path.join(bucket_name, prefix_name)
    logger.info("Listing objects in fullpath: {}".format(fullpath))
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix_name
    )
    
    # ----- process and ingest partitions into feature group
    fg_created = 0
    
    
    
    
    for content in response['Contents']:
        logger.info("... Processing partition: {}".format(content['Key'])) 
        if re.match('.*\.snappy\.parquet$', content['Key']):
            # read parquet partition
            filepath = "s3://" + os.path.join(bucket_name, content['Key'])
            df = wr.s3.read_parquet(filepath)
            logger.info("... Finished reading partition. df.shape {}".format(df.shape))

            # add event time column and cast columns to correct types
            current_time_sec = int(round(time.time()))
            df['event_time'] = pd.Series([current_time_sec] * df.shape[0], dtype="float64")
            cast_dataframe(df)
            
            # drop na in record_identifier_name
            df = df.dropna(subset=['cuit_establecimiento_host'])

            # ----- start feature ingestion
            logger.info('Feature Store ingestion start: {}'.format(
                datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))

            # check if feature group is created
            feature_group, fg_status = check_feature_group(
                dataframe=df,
                feature_group_name=feature_group_name,
                record_identifier_name='cuit_establecimiento_host',
                time_identifier_name='event_time',
                bucket_name=bucket_name_feature_store,
                prefix_name=prefix_name_feature_store,
                enable_online_store=True)

            # ingest features to feature group
            feature_group.ingest(
                data_frame=df,
                max_processes=max_processes,
                max_workers=max_workers,
                wait=False
            )

            logger.info('... partial ingestion complete: {}'.format(
                datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
            
            

    logger.info('Feature Store ingestion complete: {}'.format(
        datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
