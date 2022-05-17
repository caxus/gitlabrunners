import subprocess
import sys

subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'sagemaker==2.69.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'fsspec==2021.11.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 's3fs==2021.11.0'])

import argparse
import boto3
import csv
import json
import logging
import os
import pathlib
import sagemaker
import time
import pandas as pd
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


def cast_object_to_string(dataframe):
    for label in dataframe.columns:
        if dataframe.dtypes[label] == "object":
            dataframe[label] = dataframe[label].astype("str").astype("string")


if __name__ == "__main__":
    logger.info("Starting transformation.")

    # ---- create argparse and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_bucket", type=str)
    parser.add_argument("--input_prefix", type=str)
    parser.add_argument("--bucket_name_feature_store", type=str)
    parser.add_argument("--prefix_name_feature_store", type=str)
    parser.add_argument("--feature_group_name", type=str)
    args = parser.parse_args()

    # retrieve argument info
    logger.info("Received arguments {}".format(args))
    bucket_name = args.input_bucket
    prefix_name = args.input_prefix
    bucket_name_feature_store = args.bucket_name_feature_store
    prefix_name_feature_store = args.prefix_name_feature_store
    feature_group_name = args.feature_group_name

    # ----- read data
    logger.info("Reading data.")
    fullpath = "s3://" + os.path.join(bucket_name, prefix_name)
    print('fullpath: {}'.format(fullpath))
    df = pd.read_parquet(fullpath)
    current_time_sec = int(round(time.time()))
    df['event_time'] = pd.Series([current_time_sec] * df.shape[0],
                                 dtype="float64")
    cast_object_to_string(df)

    # ----- start feature ingestion
    logger.info('Feature Store ingestion start: {}'.format(
        datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))

    # check if feature group is created
    feature_group, fg_status = check_feature_group(
        dataframe=df,
        feature_group_name=feature_group_name,
        record_identifier_name='cuit_establecimiento_host_ref',
        time_identifier_name='event_time',
        bucket_name=bucket_name_feature_store,
        prefix_name=prefix_name_feature_store,
        enable_online_store=True)

    # ingest features to feature group
    feature_group.ingest(data_frame=df, max_workers=3, wait=True)

    logger.info('Feature Store ingestion complete: {}'.format(
        datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))