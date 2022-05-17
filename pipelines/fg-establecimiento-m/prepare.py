
import argparse
import boto3
import csv
import json
import logging
import os
import pathlib
import pyspark
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from typing import Tuple, Union, List

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# get clients
s3 = boto3.client("s3", region_name='us-east-1')
ssm = boto3.client("ssm", region_name='us-east-1')


# ==== DEFINE QUERIES
# ---- queries to filter initial data
dir_establ_hist_filter_query = """
SELECT nro_establecimiento
     , fecha_fin
     , calle
     , numero
     , codigo_postal4
     , cod_localidad
     , nombre_localidad
     , cod_tipo_direccion
     , cod_region_geografica_host
FROM direccion_establecimiento_hist 
WHERE CAST(fecha_fin as date) = '9999-12-31' 
"""

establ_hist_filter_query = """
SELECT cuit_establecimiento_host
     , nro_establecimiento
     , id_carga
     , cod_rubro
     , nombre_fantasia
     , cod_estado_establecimiento
     , cod_tipo_contrib_ganancias
     , cod_tipo_iva
     , cod_tipo_contrib_ing_brutos
     , cod_ciclo_pago_estab
     , cod_habilitacion_cuotas
     , cod_banco
     , cant_maxima_cuotas
     , cod_devol_iva
     , plataforma_alta
     , bandera
     , beneficiario_host
     , nivel_seguridad_moto_ecom
     , fecha_inicio
     , fecha_fin
     , fecha_alta
FROM establecimiento_hist 
WHERE (CAST(fecha_inicio AS date) <= '{}') AND ( (CAST(fecha_fin AS date) > '{}') OR (CAST(fecha_fin AS date) = '9999-12-31') )
""" ## fecha_inicio <= 30/11/2021, fecha_fin > 01/11/2020 OR fecha_fin IS NULL /// lastday, lastyear

mov_present_filter_query = """
SELECT fecha_movimiento
     , nro_establecimiento
     , cod_tipo_operacion
     , monto_movimiento
     , monto_movimiento_origen
FROM movimiento_presentado 
WHERE fecha_movimiento BETWEEN '{}' AND '{}'
"""  ## 01-11-2020 / 30-11-2021

# --- transformation queries
establ_sum_fmt_query = """
SELECT to_date(fecha_movimiento, 'yyyy-MM-dd')                                            AS fecha_movimiento,
       nro_establecimiento,
       SUM(CASE WHEN cod_tipo_operacion = '0005' then monto_movimiento else 0 end)        AS vol,
       SUM(CASE WHEN cod_tipo_operacion = '0510' then monto_movimiento_Origen else 0 end) AS vol_usd,
       SUM(CASE WHEN cod_tipo_operacion = '0005' then 1 else 0 end)                       AS cant,
       SUM(CASE WHEN cod_tipo_operacion = '0510' then 1 else 0 end)                       AS cant_usd
FROM movimiento_presentado
WHERE (monto_movimiento > 0) AND (fecha_movimiento between '{}' and '{}')
GROUP BY fecha_movimiento, nro_establecimiento
"""

establ_univ_tmp_query = """
SELECT DISTINCT cuit_establecimiento_host
              , nro_establecimiento
              , id_carga
              , fecha_inicio              
              , CASE WHEN (CAST(fecha_fin AS date) = '9999-12-31') THEN '1900-01-01' ELSE CAST(fecha_fin AS date) END AS fecha_fin
              
FROM establecimiento_hist
"""

establ_univ_qualify_query = """
SELECT cuit_establecimiento_host, nro_establecimiento FROM (
    SELECT cuit_establecimiento_host, nro_establecimiento, row_number() OVER (
        PARTITION BY nro_establecimiento ORDER BY id_carga DESC
    ) rank FROM GPO_AT2_universo_Establecimiento_1_tmp
) tmp WHERE rank = 1
"""

direccion_fmt_query = """
SELECT nro_establecimiento,
    H.cod_provincia,
    H.desc_provincia,
    COALESCE(G.calle,'') nombre_calle,
    COALESCE(G.numero,'') numero_calle,
    G.codigo_postal4 as codigo_postal,
    G.cod_localidad,
    G.nombre_localidad as localidad,
    CASE WHEN COALESCE(G.calle,'') = '' OR COALESCE(G.calle,'') IS NULL THEN '' ELSE COALESCE(G.calle,'') || ', ' END ||
        CASE WHEN COALESCE(G.numero,'') = '' OR COALESCE(G.numero,'') IS NULL THEN '' ELSE COALESCE(G.numero,'') || ', ' END ||
            CASE WHEN G.nombre_localidad = '' OR G.nombre_localidad IS NULL THEN '' ELSE G.nombre_localidad || ', ' END ||
                CASE WHEN H.desc_provincia = '' OR H.desc_provincia IS NULL THEN '' ELSE H.desc_provincia || ', ' END ||
                    'ARGENTINA' as direccion
FROM direccion_establecimiento_hist as G
LEFT JOIN provincia AS H ON H.cod_provincia=SUBSTR(G.cod_region_geografica_host,1,1)
WHERE G.cod_tipo_direccion='{}'
"""

udir_establ_query = """
SELECT BASE.nro_establecimiento,
        CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.nombre_calle else A.nombre_calle end nombre_calle,
        CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.numero_calle else A.numero_calle end numero_calle,
        CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.codigo_postal else A.codigo_postal end codigo_postal,
        CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.cod_localidad else A.cod_localidad end cod_localidad,
        CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.localidad else A.localidad end localidad,
        CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.cod_provincia else A.cod_provincia end cod_provincia,
        CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.desc_provincia else A.desc_provincia end desc_provincia,
        CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.direccion else A.direccion end direccion

FROM GPO_AT2_universo_Establecimiento_1 as BASE
LEFT JOIN direccion2 as A on base.nro_establecimiento = A.nro_establecimiento
LEFT JOIN direccion1 as B on base.nro_establecimiento = B.nro_establecimiento
"""

establ2_univ_query = """
SELECT DISTINCT base.cuit_establecimiento_host,
                base.nro_establecimiento,
                E.cod_rubro,
                r.desc_rubro,
                E.nombre_fantasia,
                E.cod_estado_establecimiento,
                EE.ds_estado_establecimiento AS desc_estado_establecimiento,
                E.cod_tipo_contrib_ganancias,
                TCG.desc_tipo_contrib_ganancias,
                E.cod_tipo_iva,
                TI.desc_tipo_iva,
                E.cod_tipo_contrib_ing_brutos,
                TCIB.desc_tipo_contrib_ing_brutos,
                E.cod_ciclo_pago_estab,
                CPE.desc_ciclo_pago_estab,
                E.cod_habilitacion_cuotas,
                HC.desc_habilitacion_cuotas,
                E.cod_banco,
                Banco.denominacion,
                E.cant_maxima_cuotas,
                E.cod_devol_iva,
                E.plataforma_alta,
                G.nombre_calle,
                G.numero_calle,
                G.codigo_postal,
                G.cod_localidad,
                G.localidad,
                G.cod_provincia,
                G.desc_provincia,
                G.direccion,
                CASE WHEN G.desc_provincia='BUENOS AIRES' and G2.desc_departamento_geo like '%Comuna%' then 0 else G2.latitud end latitud,
                CASE WHEN G.desc_provincia='BUENOS AIRES' and G2.desc_departamento_geo like '%Comuna%' then 0 else G2.longitud end longitud,
                CASE WHEN G.desc_provincia='BUENOS AIRES' and G2.desc_departamento_geo like '%Comuna%' then NULL else  G2.desc_departamento_geo end desc_departamento_geo,
                G2.desc_provincia_geo,
                G2.desc_localidad_gba_geo,
                E.bandera as cd_bandera,
                E.nivel_seguridad_moto_ecom   as tx_nivel_seguridad_moto_ecom  

FROM GPO_AT2_universo_Establecimiento_1 base
INNER JOIN (SELECT * FROM establecimiento_hist) as E on base.nro_establecimiento = E.nro_establecimiento
LEFT JOIN estado_establecimiento as EE on E.cod_estado_establecimiento = EE.cd_estado_establecimiento
LEFT JOIN rubro as R on E.cod_rubro = R.cod_rubro
LEFT JOIN GPO_Unificar_Direcciones_Establecimiento_Aux as G on base.nro_establecimiento = G.nro_establecimiento
LEFT JOIN direccion_establecimiento_geolocalizado_ult_inst as G2 on G2.nro_establecimiento = E.nro_establecimiento
LEFT JOIN tipo_contrib_ganancias as TCG on E.cod_tipo_contrib_ganancias = TCG.cod_tipo_contrib_ganancias
LEFT JOIN tipo_iva as TI ON E.cod_tipo_iva = TI.cod_tipo_iva
LEFT JOIN tipo_contrib_ing_brutos as TCIB ON E.cod_tipo_contrib_ing_brutos = TCIB.cod_tipo_contrib_ing_brutos
LEFT JOIN ciclo_pago_establecimiento as CPE ON E.cod_ciclo_pago_estab = CPE.cod_ciclo_pago_estab
LEFT JOIN habilitacion_cuotas as HC ON E.cod_habilitacion_cuotas = HC.cod_habilitacion_cuotas
LEFT JOIN banco as banco ON E.cod_banco = banco.cod_banco
"""

month_fmt_query = """
SELECT mp.nro_establecimiento,
    sum(cant) as cant_m{},
    sum(cant_usd) cant_usd_m{},
    sum(vol) as vol_m{},
    sum(vol_usd) as vol_usd_m{}
FROM (
    SELECT *
    FROM GPO_AT2_Establecimiento_sumarizada_mp
    WHERE fecha_movimiento BETWEEN '{}' AND '{}'
) AS mp
GROUP BY nro_establecimiento
"""

week_fmt_query = """
SELECT mp.nro_establecimiento,
sum(cant) as cant_W{},
sum(cant_usd) cant_usd_W{},
sum(vol) as vol_W{},
sum(vol_usd) as vol_usd_W{}
FROM (
    SELECT *
    FROM GPO_AT2_Establecimiento_sumarizada_mp
    WHERE fecha_movimiento BETWEEN '{}' AND '{}'
) AS mp
GROUP BY nro_establecimiento
"""

establ_txs_query = """
SELECT DISTINCT T.nro_establecimiento,
                COALESCE(SUM(m12.cant_m12), 0) AS cant_m12,
                COALESCE(SUM(m12.vol_m12), 0.0) AS vol_m12,
                COALESCE(SUM(m11.cant_m11), 0) AS cant_m11,
                COALESCE(SUM(m11.vol_m11), 0.0) AS vol_m11,
                COALESCE(SUM(m10.cant_m10), 0) AS cant_m10,
                COALESCE(SUM(m10.vol_m10), 0.0) AS vol_m10,
                COALESCE(SUM(m9.cant_m9), 0) AS cant_m9,
                COALESCE(SUM(m9.vol_m9), 0.0) AS vol_m9,
                COALESCE(SUM(m8.cant_m8), 0) AS cant_m8,
                COALESCE(SUM(m8.vol_m8), 0.0) AS vol_m8,
                COALESCE(SUM(m7.cant_m7), 0) AS cant_m7,
                COALESCE(SUM(m7.vol_m7), 0.0) AS vol_m7,
                COALESCE(SUM(m6.cant_m6), 0) AS cant_m6,
                COALESCE(SUM(m6.vol_m6), 0.0) AS vol_m6,
                COALESCE(SUM(m5.cant_m5), 0) AS cant_m5,
                COALESCE(SUM(m5.vol_m5), 0.0) AS vol_m5,
                COALESCE(SUM(m4.cant_m4), 0) AS cant_m4,
                COALESCE(SUM(m4.vol_m4), 0.0) AS vol_m4,
                COALESCE(SUM(m3.cant_m3), 0) AS cant_m3,
                COALESCE(SUM(m3.vol_m3), 0.0) AS vol_m3,
                COALESCE(SUM(m2.cant_m2), 0) AS cant_m2,
                COALESCE(SUM(m2.vol_m2), 0.0) AS vol_m2,
                COALESCE(SUM(m1.cant_m1), 0) AS cant_m1,
                COALESCE(SUM(m1.vol_m1), 0.0) AS vol_m1,
                COALESCE(SUM(w1.cant_w1), 0) AS cant_w1,
                COALESCE(SUM(w1.vol_w1), 0.0) AS vol_w1,
                COALESCE(SUM(w2.cant_w2), 0) AS cant_w2,
                COALESCE(SUM(w2.vol_w2), 0.0) AS vol_w2,
                COALESCE(SUM(w3.cant_w3), 0) AS cant_w3,
                COALESCE(SUM(w3.vol_w3), 0.0) AS vol_w3,
                COALESCE(SUM(w4.cant_w4), 0) AS cant_w4,
                COALESCE(SUM(w4.vol_w4), 0.0) AS vol_w4,
                COALESCE(sum(m12.cant_usd_m12), 0) AS cant_usd_m12,
                COALESCE(SUM(m12.vol_usd_m12), 0.0) AS vol_usd_m12,
                COALESCE(SUM(m11.cant_usd_m11), 0) AS cant_usd_m11,
                COALESCE(SUM(m11.vol_usd_m11), 0.0) AS vol_usd_m11,
                COALESCE(SUM(m10.cant_usd_m10), 0) AS cant_usd_m10,
                COALESCE(SUM(m10.vol_usd_m10), 0.0) AS vol_usd_m10,
                COALESCE(SUM(m9.cant_usd_m9), 0) AS cant_usd_m9,
                COALESCE(SUM(m9.vol_usd_m9), 0.0) AS vol_usd_m9,
                COALESCE(SUM(m8.cant_usd_m8), 0) AS cant_usd_m8,
                COALESCE(SUM(m8.vol_usd_m8), 0.0) AS vol_usd_m8,
                COALESCE(SUM(m7.cant_usd_m7), 0) AS cant_usd_m7,
                COALESCE(SUM(m7.vol_usd_m7), 0.0) AS vol_usd_m7,
                COALESCE(SUM(m6.cant_usd_m6), 0) AS cant_usd_m6,
                COALESCE(SUM(m6.vol_usd_m6), 0.0) AS vol_usd_m6,
                COALESCE(SUM(m5.cant_usd_m5), 0) AS cant_usd_m5,
                COALESCE(SUM(m5.vol_usd_m5), 0.0) AS vol_usd_m5,
                COALESCE(SUM(m4.cant_usd_m4), 0) AS cant_usd_m4,
                COALESCE(SUM(m4.vol_usd_m4), 0.0) AS vol_usd_m4,
                COALESCE(SUM(m3.cant_usd_m3), 0) AS cant_usd_m3,
                COALESCE(SUM(m3.vol_usd_m3), 0.0) AS vol_usd_m3,
                COALESCE(SUM(m2.cant_usd_m2), 0) AS cant_usd_m2,
                COALESCE(SUM(m2.vol_usd_m2), 0.0) AS vol_usd_m2,
                COALESCE(SUM(m1.cant_usd_m1), 0) AS cant_usd_m1,
                COALESCE(SUM(m1.vol_usd_m1), 0.0) AS vol_usd_m1,
                COALESCE(SUM(w1.cant_usd_w1), 0) AS cant_usd_w1,
                COALESCE(SUM(w1.vol_usd_w1), 0.0) AS vol_usd_w1,
                COALESCE(SUM(w2.cant_usd_w2), 0) AS cant_usd_w2,
                COALESCE(SUM(w2.vol_usd_w2), 0.0) AS vol_usd_w2,
                COALESCE(SUM(w3.cant_usd_w3), 0) AS cant_usd_w3,
                COALESCE(SUM(w3.vol_usd_w3), 0.0) AS vol_usd_w3,
                COALESCE(SUM(w4.cant_usd_w4), 0) AS cant_usd_w4,
                COALESCE(SUM(w4.vol_usd_w4), 0.0) AS vol_usd_w4

FROM GPO_AT2_universo_Establecimiento_2 T
LEFT JOIN month1_establecimiento_cant AS m1 on T.nro_establecimiento = m1.nro_establecimiento
LEFT JOIN month2_establecimiento_cant AS m2 on T.nro_establecimiento = m2.nro_establecimiento
LEFT JOIN month3_establecimiento_cant AS m3 on T.nro_establecimiento = m3.nro_establecimiento
LEFT JOIN month4_establecimiento_cant AS m4 on T.nro_establecimiento = m4.nro_establecimiento
LEFT JOIN month5_establecimiento_cant AS m5 on T.nro_establecimiento = m5.nro_establecimiento
LEFT JOIN month6_establecimiento_cant AS m6 on T.nro_establecimiento = m6.nro_establecimiento
LEFT JOIN month7_establecimiento_cant AS m7 on T.nro_establecimiento = m7.nro_establecimiento
LEFT JOIN month8_establecimiento_cant AS m8 on T.nro_establecimiento = m8.nro_establecimiento
LEFT JOIN month9_establecimiento_cant AS m9 on T.nro_establecimiento = m9.nro_establecimiento
LEFT JOIN month10_establecimiento_cant AS m10 on T.nro_establecimiento = m10.nro_establecimiento
LEFT JOIN month11_establecimiento_cant AS m11 on T.nro_establecimiento = m11.nro_establecimiento
LEFT JOIN month12_establecimiento_cant AS m12 on T.nro_establecimiento = m12.nro_establecimiento
LEFT JOIN week1_establecimiento_cant AS w1 on T.nro_establecimiento = w1.nro_establecimiento
LEFT JOIN week2_establecimiento_cant AS w2 on T.nro_establecimiento = w2.nro_establecimiento
LEFT JOIN week3_establecimiento_cant AS w3 on T.nro_establecimiento = w3.nro_establecimiento
LEFT JOIN week4_establecimiento_cant AS w4 on T.nro_establecimiento = w4.nro_establecimiento
GROUP BY T.nro_establecimiento
"""

establ_abt_query = """
SELECT CAST(U.cuit_establecimiento_host AS STRING)
     , CAST(U.nro_establecimiento AS INT)
     , CAST(U.cod_rubro AS STRING)
     , CAST(U.desc_rubro AS STRING) AS desc_rubro
     , CAST(U.nombre_Fantasia AS STRING)
     , CAST(U.cod_estado_establecimiento AS STRING) 
     , CAST(U.desc_estado_establecimiento AS STRING)
     , CAST(U.cod_tipo_contrib_ganancias AS STRING)
     , CAST(U.desc_tipo_contrib_ganancias AS STRING)
     , CAST(U.cod_tipo_iva AS STRING)
     , CAST(U.desc_tipo_iva AS STRING)
     , CAST(U.cod_tipo_contrib_ing_brutos AS STRING)
     , CAST(U.desc_tipo_contrib_ing_brutos AS STRING)
     , CAST(U.cod_ciclo_pago_estab AS INT)
     , CAST(U.desc_ciclo_pago_estab AS STRING)
     , CAST(U.cod_habilitacion_cuotas AS INT)
     , CAST(U.desc_habilitacion_cuotas AS STRING)
     , CAST(U.cod_banco AS INT)
     , CAST(U.denominacion AS STRING) AS desc_banco
     , CAST(U.cant_maxima_cuotas AS INT)
     , CAST(U.cod_devol_iva AS INT)
     , CAST(U.plataforma_alta AS STRING)
     , CAST(U.nombre_calle AS STRING)
     , CAST(U.numero_calle AS STRING)
     , CAST(U.codigo_postal AS STRING)
     , CAST(U.cod_localidad AS INT)
     , CAST(U.localidad AS STRING) AS nombre_localidad
     , CAST(U.cod_provincia AS STRING)
     , CAST(U.desc_provincia AS STRING)
     , CAST(U.direccion AS STRING)
     , CAST(U.latitud AS FLOAT)
     , CAST(U.longitud AS FLOAT)
     , CAST(U.desc_departamento_geo AS STRING)
     , CAST(U.desc_provincia_geo AS STRING)
     , CAST(U.desc_localidad_gba_geo AS STRING) AS desc_localidad_gba_geo
     , CAST(U.cd_bandera AS INT)
     , CAST(U.tx_nivel_seguridad_moto_ecom AS STRING)
     , CAST(T.cant_m1 AS INT)
     , CAST(T.vol_m1 AS FLOAT)
     , CAST(T.cant_usd_m1 AS INT)
     , CAST(T.vol_usd_m1 AS FLOAT)
     , CAST(T.cant_m2 AS INT)
     , CAST(T.vol_m2 AS FLOAT)
     , CAST(T.cant_usd_m2 AS INT)
     , CAST(T.vol_usd_m2 AS FLOAT)
     , CAST(T.cant_m3 AS INT)
     , CAST(T.vol_m3 AS FLOAT)
     , CAST(T.cant_usd_m3 AS INT)
     , CAST(T.vol_usd_m3 AS FLOAT)
     , CAST(T.cant_m4 AS INT)
     , CAST(T.vol_m4 AS FLOAT)
     , CAST(T.cant_usd_m4 AS INT)
     , CAST(T.vol_usd_m4 AS FLOAT)
     , CAST(T.cant_m5 AS INT)
     , CAST(T.vol_m5 AS FLOAT)
     , CAST(T.cant_usd_m5 AS INT)
     , CAST(T.vol_usd_m5 AS FLOAT)
     , CAST(T.cant_m6 AS INT)
     , CAST(T.vol_m6 AS FLOAT)
     , CAST(T.cant_usd_m6 AS INT)
     , CAST(T.vol_usd_m6 AS FLOAT)
     , CAST(T.cant_m7 AS INT)
     , CAST(T.vol_m7 AS FLOAT)
     , CAST(T.cant_usd_m7 AS INT)
     , CAST(T.vol_usd_m7 AS FLOAT)
     , CAST(T.cant_m8 AS INT)
     , CAST(T.vol_m8 AS FLOAT)
     , CAST(T.cant_usd_m8 AS INT)
     , CAST(T.vol_usd_m8 AS FLOAT)
     , CAST(T.cant_m9 AS INT)
     , CAST(T.vol_m9 AS FLOAT)
     , CAST(T.cant_usd_m9 AS INT)
     , CAST(T.vol_usd_m9 AS FLOAT)
     , CAST(T.cant_m10 AS INT)
     , CAST(T.vol_m10 AS FLOAT)
     , CAST(T.cant_usd_m10 AS INT)
     , CAST(T.vol_usd_m10 AS FLOAT)
     , CAST(T.cant_m11 AS INT)
     , CAST(T.vol_m11 AS FLOAT)
     , CAST(T.cant_usd_m11 AS INT)
     , CAST(T.vol_usd_m11 AS FLOAT)
     , CAST(T.cant_m12 AS INT)
     , CAST(T.vol_m12 AS FLOAT)
     , CAST(T.cant_usd_m12 AS INT)
     , CAST(T.vol_usd_m12 AS FLOAT)
     , CAST(T.cant_w1 AS INT)
     , CAST(T.vol_w1 AS FLOAT)
     , CAST(T.cant_usd_w1 AS INT)
     , CAST(T.vol_usd_w1 AS FLOAT)
     , CAST(T.cant_w2 AS INT)
     , CAST(T.vol_w2 AS FLOAT)
     , CAST(T.cant_usd_w2 AS INT)
     , CAST(T.vol_usd_w2 AS FLOAT)
     , CAST(T.cant_w3 AS INT)
     , CAST(T.vol_w3 AS FLOAT)
     , CAST(T.cant_usd_w3 AS INT)
     , CAST(T.vol_usd_w3 AS FLOAT)
     , CAST(T.cant_w4 AS INT)
     , CAST(T.vol_w4 AS FLOAT)
     , CAST(T.cant_usd_w4 AS INT)
     , CAST(T.vol_usd_w4 AS FLOAT)
     
FROM GPO_AT2_universo_Establecimiento_2 U
LEFT JOIN GPO_AT2_txs_estab T on T.nro_establecimiento = U.nro_establecimiento
"""


if __name__ == "__main__":
    logger.info("Starting transformation.")

    # ---- create argparse and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_bucket", type=str)
    parser.add_argument("--output_prefix", type=str)
    args = parser.parse_args()

    # add info
    logger.info("Received arguments {}".format(args))
    output_bucket = args.output_bucket
    output_prefix = args.output_prefix
    date_ref_str = (date.today()).strftime('%Y-%m-%d')

    # create spark session and spark context
    spark = SparkSession.builder.getOrCreate()
    spark_context = spark.sparkContext

    # get total cores
    total_cores = int(
        spark_context._conf.get('spark.executor.instances')) * int(
            spark_context._conf.get('spark.executor.cores'))
    logger.info(f'Total available cores in the Spark cluster = {total_cores}')

    # ----- create reference datetimes
    # create reference datetimes
    date_ref = (datetime.strptime(date_ref_str, '%Y-%m-%d')).date()
    lastday = date_ref.replace(day=1) - timedelta(days=1)
    firstday_tmp = date_ref.replace(day=1) - timedelta(days=lastday.day)
    firstday = firstday_tmp
    firstday = firstday_tmp - relativedelta(months=12)
    lastyear = firstday_tmp - relativedelta(months=12)
    logger.info("date_ref {}".format(date_ref))
    logger.info("firstday {}".format(firstday))
    logger.info("lastday {}".format(lastday))
    logger.info("lastyear {}".format(lastyear))

    # ----- read data
    logger.info("Reading data.")

    # define basepath and queries for each table
    sdfs = {}
    table_filter_queries = {
        "at_inflacion_202006": {
            "ssm_param_name": "S3PATH_INFLACION_TABLE",
            "filter_query": None,
        },
        "banco": {
            "ssm_param_name": "S3PATH_BANCO_TABLE",
            "filter_query": None,
        },
        "ciclo_pago_establecimiento": {
            "ssm_param_name": "S3PATH_CICLO_PAGO_ESTABLECIMIENTO_TABLE",
            "filter_query": None,
        }, 
        "direccion_establecimiento_geolocalizado_ult_inst": {
            "ssm_param_name": "S3PATH_DIR_ESTABLECIMIENTO_GEO_ULT_INST_TABLE",
            "filter_query": None,
        },
        "direccion_establecimiento_hist": {
            "ssm_param_name": "S3PATH_DIR_ESTABLECIMIENTO_HIST_TABLE",
            "filter_query": None,
        },
        "establecimiento_hist": {
            "ssm_param_name": "S3PATH_ESTABLECIMIENTO_HIST_TABLE",
            "filter_query": establ_hist_filter_query.format(lastday, lastyear),
        },
        "estado_establecimiento": {
            "ssm_param_name": "S3PATH_ESTADO_ESTABLECIMIENTO_TABLE",
            "filter_query": None,
        },
        "habilitacion_cuotas": {
            "ssm_param_name": "S3PATH_HABILITACION_CUOTAS_TABLE",
            "filter_query": None,
        },
        "movimiento_presentado": {
            "ssm_param_name": "S3PATH_MOVIMIENTO_PRESENTADO_TABLE",
            "filter_query": mov_present_filter_query.format(lastyear, lastday),
        },
        "provincia": {
            "ssm_param_name": "S3PATH_PROVINCIA_TABLE",
            "filter_query": None,
        },
        "rubro": {
            "ssm_param_name": "S3PATH_RUBRO_TABLE",
            "filter_query": None,
        },
        "tipo_contrib_ganancias": {
            "ssm_param_name": "S3PATH_TIPO_CONTRIB_GANANCIAS_TABLE",
            "filter_query": None,
        },
        "tipo_contrib_ing_brutos": {
            "ssm_param_name": "S3PATH_TIPO_CONTRIB_ING_BRUTOS_TABLE",
            "filter_query": None,
        },
        "tipo_iva": {
            "ssm_param_name": "S3PATH_TIPO_IVA_TABLE",
            "filter_query": None,
        },
    }

    for table, query_dict in table_filter_queries.items():
        print('table: {}'.format(table))
        tablepath = ssm.get_parameter(Name=query_dict['ssm_param_name'])['Parameter']['Value']
        sdfs[table] = spark.read.parquet(tablepath)
        sdfs[table].createOrReplaceTempView(table)  # create temp view
        logger.info("Processed table: {}".format(table))
        if query_dict['filter_query'] is not None:
            sdfs[table] = spark.sql(query_dict['filter_query'])
            sdfs[table].createOrReplaceTempView(table)
            logger.info("Filtered table: {}".format(table))

    # ----- create `GPO_AT2_Establecimiento_sumarizada_mp` view
    # execute query
    establ_sum_sdf = spark.sql(establ_sum_fmt_query.format(lastyear, lastday))

    # add cod_mes column
    establ_sum_sdf = establ_sum_sdf.withColumn(
        "cod_mes", (year(col("fecha_movimiento")) * 100) + (
            month(col("fecha_movimiento"))
        )
    )

    # register temporary view
    establ_sum_sdf.createOrReplaceTempView(
        "GPO_AT2_Establecimiento_sumarizada_mp"
    )

    # ----- create `GPO_AT2_Establecimiento_sumarizada_mp` view
    logger.info("Creating GPO_AT2_Establecimiento_sumarizada_mp view.")

    # execute query
    establ_univ_tmp_sdf = spark.sql(establ_univ_tmp_query)

    # filter date
#     establ_univ_tmp_sdf = establ_univ_tmp_sdf.filter(
#         (col('fecha_inicio') < lastday) & (
#             (col('fecha_fin') < firstday) | (col('fecha_fin').isNull())))

    # select columns
    establ_univ_tmp_sdf = establ_univ_tmp_sdf.select(
        "cuit_establecimiento_host",
        "nro_establecimiento",
        "id_carga"
    )

    # register temporary view
    establ_univ_tmp_sdf.createOrReplaceTempView(
        "GPO_AT2_universo_Establecimiento_1_tmp"
    )

    # apply qualify partition over nro_establ
    establ_univ_sdf = spark.sql(establ_univ_qualify_query)

    # register temporary view of the final table
    establ_univ_sdf.createOrReplaceTempView(
        "GPO_AT2_universo_Establecimiento_1"
    )

    # ----- CREATE `GPO_Unificar_Direcciones_Establecimiento_Aux` view
    logger.info("Creating GPO_Unificar_Direcciones_Establecimiento_Aux view.")

    # create auxiliary tables for directions 1 and 2
    for direction in [1, 2]:
        # execute query for direction
        loc_sdf = spark.sql(direccion_fmt_query.format(direction))

        # register temporary view
        loc_sdf.createOrReplaceTempView("direccion{}".format(direction))

    # create the final table using directions 1 and 2
    # execute query and register view
    udir_establ_sdf = spark.sql(udir_establ_query)
    udir_establ_sdf.createOrReplaceTempView(
        'GPO_Unificar_Direcciones_Establecimiento_Aux'
    )

    # ----- create `GPO_AT2_universo_Establecimiento_2` temp view
    logger.info("Creating GPO_AT2_universo_Establecimiento_2 view.")

    # execute query and register view
    establ2_univ_sdf = spark.sql(establ2_univ_query)
    establ2_univ_sdf.createOrReplaceTempView(
        'GPO_AT2_universo_Establecimiento_2'
    )

    # ----- create aux table counting establishments for each month
    logger.info("Create aux tables counting establishments for each month.")
    #date_ref_aux = date_ref
    # date_ref_aux = date(year=2021, month=1, day=1)
    delta_period = 0
    date_ref_aux = date_ref + relativedelta(months=delta_period) + timedelta(days=-delta_period)

    for month_ref in range(1, 14):
        logger.info('Creating aggregated table for month {}'.format(month_ref))
        # update month string to pass as variable name
        month_name = month_ref #-1 if month_ref != 0 else ''

        # define table name
        month_table_name = 'month{}_establecimiento_cant'.format(month_name)

        # get month references
        month_end = date_ref_aux.replace(day=1) - timedelta(days=1)
        month_start = date_ref_aux.replace(day=1) - timedelta(days=month_end.day)
        date_ref_aux = month_end  # update date_ref_aux
        logger.info("month_start: {}".format(month_start))
        logger.info("month_end: {}".format(month_end))
        
        # execute the query
        month_sdf = spark.sql(month_fmt_query.format(
            month_name,
            month_name,
            month_name,
            month_name,
            month_start,
            month_end
        ))

        # create the temporary view
        month_sdf.createOrReplaceTempView(month_table_name)

    # ----- create aux table counting establishments for each week
    logger.info("Create aux tables counting establishments for each week.")
    fecha_inicio_week = lastday - timedelta(days=6)
    fecha_fin_week = lastday
    
    for week_ref in range(1, 5):
        logger.info('Creating aggregated table for week {}'.format(week_ref))
        # update month string to pass as variable name
        week_name = week_ref

        # define table name
        week_autoritation_table_name = 'week{}_establecimiento_cant'.format(week_name)
        logger.info('week_name: {}'.format(week_name))
        logger.info('fecha_inicio_week: {}'.format(fecha_inicio_week))
        logger.info('fecha_fin_week: {}'.format(fecha_fin_week))

        week_sdf = spark.sql(week_fmt_query.format(
            week_name,
            week_name,
            week_name,
            week_name,
            fecha_inicio_week,
            fecha_fin_week
        ))

        fecha_inicio_week = fecha_inicio_week - timedelta(days=7)
        fecha_fin_week = fecha_fin_week - timedelta(days=7)

        # create the temporary view
        week_sdf.createOrReplaceTempView(week_autoritation_table_name)

    # ----- create `GPO_AT2_txs_estab` view
    logger.info("Creating GPO_AT2_txs_estab view.")

    # execute query and register view
    txs_estab_sdf = spark.sql(establ_txs_query)
    txs_estab_sdf.createOrReplaceTempView('GPO_AT2_txs_estab')

    # ----- finally, create `ABT_Establecimiento_M`
    logger.info("Creating ABT_Establecimiento_M view.")

    # execute query and register view
    establ_abt_sdf = spark.sql(establ_abt_query)
    establ_abt_sdf = establ_abt_sdf.withColumn("fe_proceso", current_date())
    establ_abt_sdf = establ_abt_sdf.withColumn("id_periodo_analisis", date_format(add_months(to_date(col("fe_proceso"),"yyyy-MM"),-1),"yyyy-MM"))  
    establ_abt_sdf.createOrReplaceTempView('ABT_Establecimiento_M')

    # ----- transform spark dataframe to pandas and write to parquet
    logger.info("Writing ABT_Establecimiento_M to parquet.")
    # repartition dataframe
    establ_abt_sdf = establ_abt_sdf.repartition(total_cores * 3)

    # write to output
    output_path = "s3://" + os.path.join(
        output_bucket, output_prefix, "establecimiento_dataset.snappy.parquet")
    establ_abt_sdf.write.mode('overwrite').parquet(output_path, compression='snappy')

