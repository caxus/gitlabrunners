
"""
Data ingestion for Activacion Terminales 2.
This file produces the model database for Activación de Terminales 2.
"""

import subprocess
import sys

subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'sagemaker'])

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
from dateutil.relativedelta import relativedelta
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
    feature_group = FeatureGroup(
        name=feature_group_name,
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
        return (feature_group, feature_group.describe().get("FeatureGroupStatus"))

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
    parser.add_argument("--output_feature_group_name", type=str)
    parser.add_argument("--bucket_name_feature_store", type=str)
    parser.add_argument("--prefix_name_feature_store", type=str)
    parser.add_argument("--sql_output_path", type=str)
    parser.add_argument("--enable_online_store", type=bool)
    args = parser.parse_args()

    # retrieve argument info
    logger.info("Received arguments {}".format(args))
    bucket_name_feature_store = args.bucket_name_feature_store
    prefix_name_feature_store = args.prefix_name_feature_store
    output_feature_group_name = args.output_feature_group_name
    sql_output_path = args.sql_output_path
    enable_online_store = bool(args.enable_online_store)  # verificar si es necesario escribir bool(enable_online_store)

    # get cod_mes_ref para generar la referencia de variables para el mes
    date_ref_str = (date.today()).strftime('%Y-%m-%d')
    date_ref = (datetime.strptime(date_ref_str, '%Y-%m-%d')).date()
    delta_period = -1  # para restar un mes a la fecha actual
    date_ref_aux = date_ref + relativedelta(months=delta_period) + timedelta(days=-delta_period)
    cod_mes_pred_ref = date_ref_aux.strftime('%Y-%m')
    
    

    # ----- create database from fg-establecimiento-principal-m
    logger.info("Create database cod_mes_pred_ref {} using sql query on fg-establecimiento-principal-m.".format(
        cod_mes_pred_ref))

    # instantiate fg-establecimiento-principal
    fg_establ_principal = FeatureGroup(
        name='fg-establecimiento-principal-m',
        sagemaker_session=sagemaker_session)

    # instantiate fg-activterm-target
    fg_target = FeatureGroup(
        name='fg-activterm-target-m', 
        sagemaker_session=sagemaker_session)

    # instantiate fg-activterm-target
    fg_establ = FeatureGroup(
        name='fg-establecimiento-m', 
        sagemaker_session=sagemaker_session)
    
    # get fg-establecimiento-principal status [TO-DO: INCLUIR TRY-EXCEPT PARA RESOURCENOTFOUND]
    fg_estbl_principal_status = fg_establ_principal.describe().get("FeatureGroupStatus")

    # instantiate athena query
    fg_establ_princip_query = fg_establ_principal.athena_query()
    fg_establ_princip_table = fg_establ_princip_query.table_name

    fg_target_query = fg_target.athena_query()
    fg_target_table = fg_target_query.table_name

    fg_establ_query = fg_establ.athena_query()
    fg_establ_table = fg_establ_query.table_name

    # define athena query
    sql_query = f"""
    SELECT ep.cuit_establecimiento_host_ref,
            ep.id_periodo_analisis,
            r.cod_rubro,
            r.cant_txs_m1_rubro,
            r.vol_txs_m1_rubro,
            r.cant_txs_m2_rubro,
            r.vol_txs_m2_rubro,
            r.cant_txs_m3_rubro,
            r.vol_txs_m3_rubro,
            r.cant_txs_m4_rubro,
            r.vol_txs_m4_rubro,
            r.cant_txs_m5_rubro,
            r.vol_txs_m5_rubro,
            r.cant_txs_m6_rubro,
            r.vol_txs_m6_rubro,
            r.cant_txs_m7_rubro,
            r.vol_txs_m7_rubro,
            r.cant_txs_m8_rubro,
            r.vol_txs_m8_rubro,
            r.cant_txs_m9_rubro,
            r.vol_txs_m9_rubro,
            r.cant_txs_m10_rubro,
            r.vol_txs_m10_rubro,
            r.cant_txs_m11_rubro,
            r.vol_txs_m11_rubro,
            r.cant_txs_m12_rubro,
            r.vol_txs_m12_rubro,
            p.cant_txs_m12_prov,
            p.vol_txs_m12_prov,
            p.cant_txs_m11_prov,
            p.vol_txs_m11_prov,
            p.cant_txs_m10_prov,
            p.vol_txs_m10_prov,
            p.cant_txs_m9_prov,
            p.vol_txs_m9_prov,
            p.cant_txs_m8_prov,
            p.vol_txs_m8_prov,
            p.cant_txs_m7_prov,
            p.vol_txs_m7_prov,
            p.cant_txs_m6_prov,
            p.vol_txs_m6_prov,
            p.cant_txs_m5_prov,
            p.vol_txs_m5_prov,
            p.cant_txs_m4_prov,
            p.vol_txs_m4_prov,
            p.cant_txs_m3_prov,
            p.vol_txs_m3_prov,
            p.cant_txs_m2_prov,
            p.vol_txs_m2_prov,
            p.cant_txs_m1_prov,
            p.vol_txs_m1_prov,
            rp.cant_txs_m1_rubro_prov,
            rp.vol_txs_m1_rubro_prov,
            rp.cant_txs_m2_rubro_prov,
            rp.vol_txs_m2_rubro_prov,
            rp.cant_txs_m3_rubro_prov,
            rp.vol_txs_m3_rubro_prov,
            rp.cant_txs_m4_rubro_prov,
            rp.vol_txs_m4_rubro_prov,
            rp.cant_txs_m5_rubro_prov,
            rp.vol_txs_m5_rubro_prov,
            rp.cant_txs_m6_rubro_prov,
            rp.vol_txs_m6_rubro_prov,
            rp.cant_txs_m7_rubro_prov,
            rp.vol_txs_m7_rubro_prov,
            rp.cant_txs_m8_rubro_prov,
            rp.vol_txs_m8_rubro_prov,
            rp.cant_txs_m9_rubro_prov,
            rp.vol_txs_m9_rubro_prov,
            rp.cant_txs_m10_rubro_prov,
            rp.vol_txs_m10_rubro_prov,
            rp.cant_txs_m11_rubro_prov,
            rp.vol_txs_m11_rubro_prov,
            rp.cant_txs_m12_rubro_prov,
            rp.vol_txs_m12_rubro_prov,
            rr.avg_dias_1tx_rubro,
            rr.avg_dias_85000_rubro,
            pp.avg_dias_1tx_prov,
            pp.avg_dias_85000_prov,
            rrpp.avg_dias_1tx_rubro_prov,
            rrpp.avg_dias_85000_rubro_prov,
            lp.avg_dias_1tx_loc_prov,
            lp.avg_dias_85000_loc_prov,
            geo.cant_txs_m1_geo,
            geo.vol_txs_m1_geo,
            geo.cant_txs_m2_geo,
            geo.vol_txs_m2_geo,
            geo.cant_txs_m3_geo,
            geo.vol_txs_m3_geo,
            geo.cant_txs_m4_geo,
            geo.vol_txs_m4_geo,
            geo.cant_txs_m5_geo,
            geo.vol_txs_m5_geo,
            geo.cant_txs_m6_geo,
            geo.vol_txs_m6_geo,
            geo.cant_txs_m7_geo,
            geo.vol_txs_m7_geo,
            geo.cant_txs_m8_geo,
            geo.vol_txs_m8_geo,
            geo.cant_txs_m9_geo,
            geo.vol_txs_m9_geo,
            geo.cant_txs_m10_geo,
            geo.vol_txs_m10_geo,
            geo.cant_txs_m11_geo,
            geo.vol_txs_m11_geo,
            geo.cant_txs_m12_geo,
            geo.vol_txs_m12_geo,
            t.monto_terminal,
            t.clase
    FROM "sagemaker_featurestore"."{fg_establ_princip_table}" AS ep

    LEFT JOIN (
        SELECT cod_rubro AS cod_rubro,
            AVG(cant_txs_m1) AS cant_txs_m1_rubro,
            AVG(vol_txs_m1) AS vol_txs_m1_rubro,
            AVG(cant_txs_m2) AS cant_txs_m2_rubro,
            AVG(vol_txs_m2) AS vol_txs_m2_rubro,
            AVG(cant_txs_m3) AS cant_txs_m3_rubro,
            AVG(vol_txs_m3) AS vol_txs_m3_rubro,
            AVG(cant_txs_m4) AS cant_txs_m4_rubro,
            AVG(vol_txs_m4) AS vol_txs_m4_rubro,
            AVG(cant_txs_m5) AS cant_txs_m5_rubro,
            AVG(vol_txs_m5) AS vol_txs_m5_rubro,
            AVG(cant_txs_m6) AS cant_txs_m6_rubro,
            AVG(vol_txs_m6) AS vol_txs_m6_rubro,
            AVG(cant_txs_m7) AS cant_txs_m7_rubro,
            AVG(vol_txs_m7) AS vol_txs_m7_rubro,
            AVG(cant_txs_m8) AS cant_txs_m8_rubro,
            AVG(vol_txs_m8) AS vol_txs_m8_rubro,
            AVG(cant_txs_m9) AS cant_txs_m9_rubro,
            AVG(vol_txs_m9) AS vol_txs_m9_rubro,
            AVG(cant_txs_m10) AS cant_txs_m10_rubro,
            AVG(vol_txs_m10) AS vol_txs_m10_rubro,
            AVG(cant_txs_m11) AS cant_txs_m11_rubro,
            AVG(vol_txs_m11) AS vol_txs_m11_rubro,
            AVG(cant_txs_m12) AS cant_txs_m12_rubro,
            AVG(vol_txs_m12) AS vol_txs_m12_rubro      
        FROM "sagemaker_featurestore"."{fg_establ_princip_table}"
        WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}')
        GROUP BY cod_rubro
        ) AS r ON r.cod_rubro = ep.cod_rubro

    LEFT JOIN (
        SELECT cod_provincia AS cod_provincia,
            AVG(cant_txs_m1) AS cant_txs_m1_prov,
            AVG(vol_txs_m1) AS vol_txs_m1_prov,
            AVG(cant_txs_m2) AS cant_txs_m2_prov,
            AVG(vol_txs_m2) AS vol_txs_m2_prov,
            AVG(cant_txs_m3) AS cant_txs_m3_prov,
            AVG(vol_txs_m3) AS vol_txs_m3_prov,
            AVG(cant_txs_m4) AS cant_txs_m4_prov,
            AVG(vol_txs_m4) AS vol_txs_m4_prov,
            AVG(cant_txs_m5) AS cant_txs_m5_prov,
            AVG(vol_txs_m5) AS vol_txs_m5_prov,
            AVG(cant_txs_m6) AS cant_txs_m6_prov,
            AVG(vol_txs_m6) AS vol_txs_m6_prov,
            AVG(cant_txs_m7) AS cant_txs_m7_prov,
            AVG(vol_txs_m7) AS vol_txs_m7_prov,
            AVG(cant_txs_m8) AS cant_txs_m8_prov,
            AVG(vol_txs_m8) AS vol_txs_m8_prov,
            AVG(cant_txs_m9) AS cant_txs_m9_prov,
            AVG(vol_txs_m9) AS vol_txs_m9_prov,
            AVG(cant_txs_m10) AS cant_txs_m10_prov,
            AVG(vol_txs_m10) AS vol_txs_m10_prov,
            AVG(cant_txs_m11) AS cant_txs_m11_prov,
            AVG(vol_txs_m11) AS vol_txs_m11_prov,
            AVG(cant_txs_m12) AS cant_txs_m12_prov,
            AVG(vol_txs_m12) AS vol_txs_m12_prov
        FROM "sagemaker_featurestore"."{fg_establ_princip_table}"
        WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}')
        GROUP BY cod_provincia
        ) AS p ON (p.cod_provincia = ep.cod_provincia)

    LEFT JOIN (
        SELECT cod_rubro AS cod_rubro, 
            cod_provincia AS cod_provincia,
            AVG(cant_txs_m1) AS cant_txs_m1_rubro_prov,
            AVG(vol_txs_m1) AS vol_txs_m1_rubro_prov,
            AVG(cant_txs_m2) AS cant_txs_m2_rubro_prov,
            AVG(vol_txs_m2) AS vol_txs_m2_rubro_prov,
            AVG(cant_txs_m3) AS cant_txs_m3_rubro_prov,
            AVG(vol_txs_m3) AS vol_txs_m3_rubro_prov,
            AVG(cant_txs_m4) AS cant_txs_m4_rubro_prov,
            AVG(vol_txs_m4) AS vol_txs_m4_rubro_prov,
            AVG(cant_txs_m5) AS cant_txs_m5_rubro_prov,
            AVG(vol_txs_m5) AS vol_txs_m5_rubro_prov,
            AVG(cant_txs_m6) AS cant_txs_m6_rubro_prov,
            AVG(vol_txs_m6) AS vol_txs_m6_rubro_prov,
            AVG(cant_txs_m7) AS cant_txs_m7_rubro_prov,
            AVG(vol_txs_m7) AS vol_txs_m7_rubro_prov,
            AVG(cant_txs_m8) AS cant_txs_m8_rubro_prov,
            AVG(vol_txs_m8) AS vol_txs_m8_rubro_prov,
            AVG(cant_txs_m9) AS cant_txs_m9_rubro_prov,
            AVG(vol_txs_m9) AS vol_txs_m9_rubro_prov,
            AVG(cant_txs_m10) AS cant_txs_m10_rubro_prov,
            AVG(vol_txs_m10) AS vol_txs_m10_rubro_prov,
            AVG(cant_txs_m11) AS cant_txs_m11_rubro_prov,
            AVG(vol_txs_m11) AS vol_txs_m11_rubro_prov,
            AVG(cant_txs_m12) AS cant_txs_m12_rubro_prov,
            AVG(vol_txs_m12) AS vol_txs_m12_rubro_prov
        FROM "sagemaker_featurestore"."{fg_establ_princip_table}"
        WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}')
        GROUP BY cod_rubro, cod_provincia
    ) AS rp ON (rp.cod_provincia = ep.cod_provincia AND rp.cod_rubro = ep.cod_rubro)

    LEFT JOIN ( 
        SELECT cod_rubro AS cod_rubro, 
            SUM(avg_dias_1tx) AS avg_dias_1tx_rubro, 
            SUM(avg_dias_85000) AS avg_dias_85000_rubro 
        FROM "sagemaker_featurestore"."{fg_establ_princip_table}" 
        WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}') 
        GROUP BY cod_rubro 
    ) AS rr ON rr.cod_rubro = ep.cod_rubro

    LEFT JOIN (
        SELECT cod_provincia AS cod_provincia,
            SUM(avg_dias_1tx) AS avg_dias_1tx_prov,
            SUM(avg_dias_85000) AS avg_dias_85000_prov
        FROM "sagemaker_featurestore"."{fg_establ_princip_table}"
        WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}')
        GROUP BY cod_provincia
    ) AS pp ON (pp.cod_provincia = ep.cod_provincia)


    LEFT JOIN ( 
        SELECT cod_rubro AS cod_rubro, 
            cod_provincia AS cod_provincia, 
            SUM(avg_dias_1tx) AS avg_dias_1tx_rubro_prov, 
            SUM(avg_dias_85000) AS avg_dias_85000_rubro_prov
        FROM "sagemaker_featurestore"."{fg_establ_princip_table}"
        WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}')
        GROUP BY cod_rubro, cod_provincia
    ) AS rrpp ON (rrpp.cod_provincia = ep.cod_provincia AND rrpp.cod_rubro = ep.cod_rubro)


    LEFT JOIN (
        SELECT cod_localidad AS cod_localidad,
            cod_provincia AS cod_provincia,
            SUM(avg_dias_1tx) AS avg_dias_1tx_loc_prov,
            SUM(avg_dias_85000) AS avg_dias_85000_loc_prov 
        FROM "sagemaker_featurestore"."{fg_establ_princip_table}"
        WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}')
        GROUP BY cod_localidad, cod_provincia
    ) AS lp ON (lp.cod_provincia = ep.cod_provincia AND lp.cod_localidad = ep.cod_localidad)


    LEFT JOIN (
        SELECT cuit_establecimiento_host, clase, monto_terminal
        FROM "sagemaker_featurestore"."{fg_target_table}"
        WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}')
    ) AS t ON (t.cuit_establecimiento_host = ep.cuit_establecimiento_host_ref)


    LEFT JOIN (
        SELECT est1.cuit_establecimiento_host as cuit_establecimiento_host,
            AVG(cant_m1) AS cant_txs_m1_geo,
            AVG(vol_m1) AS vol_txs_m1_geo,
               AVG(cant_m2) AS cant_txs_m2_geo,
               AVG(vol_m2) AS vol_txs_m2_geo,
               AVG(cant_m3) AS cant_txs_m3_geo,
               AVG(vol_m3) AS vol_txs_m3_geo,
               AVG(cant_m4) AS cant_txs_m4_geo,
               AVG(vol_m4) AS vol_txs_m4_geo,
               AVG(cant_m5) AS cant_txs_m5_geo,
               AVG(vol_m5) AS vol_txs_m5_geo,
               AVG(cant_m6) AS cant_txs_m6_geo,
               AVG(vol_m6) AS vol_txs_m6_geo,
               AVG(cant_m7) AS cant_txs_m7_geo,
               AVG(vol_m7) AS vol_txs_m7_geo,
               AVG(cant_m8) AS cant_txs_m8_geo,
               AVG(vol_m8) AS vol_txs_m8_geo,
               AVG(cant_m9) AS cant_txs_m9_geo,
               AVG(vol_m9) AS vol_txs_m9_geo,
               AVG(cant_m10) AS cant_txs_m10_geo,
               AVG(vol_m10) AS vol_txs_m10_geo,
               AVG(cant_m11) AS cant_txs_m11_geo,
               AVG(vol_m11) AS vol_txs_m11_geo,
               AVG(cant_m12) AS cant_txs_m12_geo,
               AVG(vol_m12) AS vol_txs_m12_geo

        FROM (
            SELECT DISTINCT cuit_establecimiento_host, 
                latitud, 
                longitud 
            FROM "{fg_establ_table}" 
            WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}') 
    ) AS est1 
    CROSS JOIN ( 
        SELECT DISTINCT cuit_establecimiento_host,
                    latitud,
                    longitud,
                    cant_m1,
                    cant_m2,
                    cant_m3,
                    cant_m4,
                    cant_m5,
                    cant_m6,
                    cant_m7,
                    cant_m8,
                    cant_m9,
                    cant_m10,
                    cant_m11,
                    cant_m12,
                    vol_m1,
                    vol_m2,
                    vol_m3,
                    vol_m4,
                    vol_m5,
                    vol_m6,
                    vol_m7,
                    vol_m8,
                    vol_m9,
                    vol_m10,
                    vol_m11,
                    vol_m12
        FROM "{fg_establ_table}" 
        WHERE id_periodo_analisis IN ('{cod_mes_pred_ref}') 
    ) AS est2 
    
    WHERE (great_circle_distance(est1.latitud, est1.longitud, est2.latitud, est2.longitud) <= 5.0) AND (est1.cuit_establecimiento_host <> est2.cuit_establecimiento_host)
    GROUP BY 1
    ) AS geo ON geo.cuit_establecimiento_host = ep.cuit_establecimiento_host_ref

    WHERE ep.id_periodo_analisis IN ('{cod_mes_pred_ref}')
    """

    # execute query
    fg_establ_query.run(query_string=sql_query, output_location=sql_output_path)
    fg_establ_query.wait()
    database_df = fg_establ_query.as_dataframe()
    logger.info('database_df.shape after query execution: {}'.format(database_df.shape))

    # create event_time column
    current_time_sec = int(round(time.time()))
    database_df['event_time'] = pd.Series([current_time_sec] * database_df.shape[0], dtype="float64")
    logger.info('current_time_sec used in envet_time: {}'.format(current_time_sec))

    # cast columns
    cast_object_to_string(database_df)
    logger.info('Executed cast on dataframe.')

    # ----- start feature ingestion
    logger.info('Feature Store ingestion start: {}'.format(
        datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))

    # check if feature group is created
    feature_group, fg_status = check_feature_group(
        dataframe=database_df,
        feature_group_name=output_feature_group_name,
        record_identifier_name='cuit_establecimiento_host_ref',  # VERIFICAR ID / CUIT_ESTABLECIMIENTO_REF
        time_identifier_name='event_time',
        bucket_name=bucket_name_feature_store,
        prefix_name=prefix_name_feature_store,
        enable_online_store=enable_online_store)

    # ingest features to feature group
    feature_group.ingest(data_frame=database_df, max_workers=3, wait=True)

    logger.info('Feature Store ingestion complete: {}'.format(
        datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
