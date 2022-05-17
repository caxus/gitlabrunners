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
# create spark session
#spark = SparkSession.builder.getOrCreate()

# get clients
s3 = boto3.client("s3", region_name='us-east-1')
ssm = boto3.client("ssm", region_name='us-east-1')

# ==== DEFINE QUERIES
# ---- queries to filter initial data
autorizacion_filter_query = """
SELECT nro_terminal
     , nro_establecimiento_orig
     , fecha_host_entrada_autoriz
     , cod_respuesta_autorizacion
     , cod_tipo_mensaje_autoriz
     , nro_autorizacion
     , monto_autoriz_moneda_orig
     , programa_periferia_origen
     , datos_adicionales_2
FROM autorizacion
WHERE fecha_host_entrada_autoriz BETWEEN date '{}' AND date '{}'
""" ## lastyear and lastday

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
WHERE (CAST(fecha_inicio AS date) <= date '{}') AND ( (CAST(fecha_fin AS date) > date '{}') OR (CAST(fecha_fin AS date) = date '9999-12-31') )
""" ## lastday, lastyear

# --- transformation queries
establ_sum_fmt_query = """
SELECT A.nro_terminal,
       A.nro_establecimiento_orig,
       A.fecha_host_entrada_autoriz,
       SUM(A.monto_autoriz_moneda_orig)  AS volumen,
       COUNT(*)                          AS cantidad
FROM autorizacion A
WHERE (A.cod_respuesta_autorizacion IN (0, 1, 4, 7, 400) OR (A.cod_respuesta_autorizacion=900 AND TRIM(A.nro_autorizacion) <>'')) --> aprobadas
      AND A.cod_tipo_mensaje_autoriz=1100 
      AND programa_periferia_origen BETWEEN 530000 AND 549999 --> Adquirencia MC Prisma (saco la emision dup).
      AND A.nro_terminal NOT IN ('17010499') --> saco terminal prueba.
      AND monto_autoriz_moneda_orig>0
      AND (cast(fecha_host_entrada_autoriz as date) between date '{}' and date '{}')
GROUP BY nro_terminal, nro_establecimiento_orig, fecha_host_entrada_autoriz
"""

# Listar Establecimientos
establ_Prin_Campos_Query = """
SELECT cuit_establecimiento_host,
       nro_establecimiento,
       e.id_carga,
       e.fecha_inicio,
       e.fecha_fin
       
FROM establecimiento_hist e
LEFT JOIN estado_establecimiento EE on E.cod_estado_establecimiento = EE.cd_estado_establecimiento

WHERE (CAST(fecha_inicio AS date) <= date '{}') AND ( (CAST(fecha_fin AS date) > date '{}') OR (CAST(fecha_fin AS date) = date '9999-12-31') )
"""

establ_Prin_Campos_qualify_query = """
SELECT cuit_establecimiento_host,
       nro_establecimiento
FROM (
    SELECT cuit_establecimiento_host,
           nro_establecimiento,
           row_number() OVER (PARTITION BY nro_establecimiento ORDER BY id_carga DESC) rank
    FROM GPO_AT2_universo_Establecimiento_1_tmp
) tmp WHERE rank = 1
"""

Parque_Instalado_Aux_1_1 = """
SELECT terminalid,
       fecha_instalacion_1,
       resultado_instalacion
FROM (
    SELECT terminalid,
           dfechalogging AS fecha_instalacion_1,
           postestadoid  AS resultado_instalacion,
           row_number() OVER (PARTITION BY terminalid ORDER BY dfechalogging ASC) rank 
    FROM lapos_loguser
) tmp 
WHERE rank = 1
"""

Parque_Instalado_Aux_1_2 = """
SELECT nro_terminal,
       MAX(fecha_host_entrada_autoriz)                                              AS ultima_fecha_actividad,
       DATEDIFF(date '{}', MAX(fecha_host_entrada_autoriz))                              AS dias_sin_actividad,
       MIN(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='W' THEN 1
                        WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='G' THEN 2
                        WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='E' THEN 3
                        WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='D' THEN 4
                        ELSE 9 END)                                                 AS tipo_tecnologia, -- Este es el orden de prioridad de las tecnologias
    SUM(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='W' THEN 1 ELSE 0 end)    AS q_tx_wifi,
    SUM(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='G' THEN 1 ELSE 0 end)    AS q_tx_gsm,
    SUM(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='E' THEN 1 ELSE 0 end)    AS q_tx_ethernet,
    SUM(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='D' THEN 1 ELSE 0 end)    AS q_tx_dialup
FROM autorizacion AS A
WHERE (A.cod_respuesta_autorizacion IN (0, 1, 4, 7, 400)  OR  (A.cod_respuesta_autorizacion = 900 AND TRIM(A.nro_autorizacion) <> ''))
      AND (monto_autoriz_moneda_orig >= 300) 
      AND SUBSTR(A.nro_terminal,1, 2) IN ('07', '09','10','11','12','17','18','31','32','33','34','35','36','47','48','49','13','14','15','16', '51') 
GROUP BY 1
"""

Parque_Instalado_Aux_1_Query = """
SELECT DISTINCT A.nro_terminal,
                A.fecha_instalacion,
                A.resultado_instalacion,
                A.ultima_fecha_actividad,
                CASE 
                    WHEN  meses_de_inactividad <= 1 THEN 'Activo'
                    WHEN  meses_de_inactividad BETWEEN 1 AND 2.9999 THEN '1 a 3'
                    WHEN  meses_de_inactividad BETWEEN 3.0000 AND 5.9999 THEN '3 a 6'
                    WHEN  meses_de_inactividad BETWEEN 6.0000 AND 8.9999 THEN '6 a 9'
                    WHEN  meses_de_inactividad BETWEEN 9.0000 AND 11.9999 THEN '9 a 12'
                    WHEN  meses_de_inactividad >= 12.0000 THEN 'Mas de 12'
                     END AS tipo_inactividad,
                CASE WHEN tipo_tecnologia =1 THEN 'WIFI'
                    WHEN tipo_tecnologia =2 THEN 'GSM'
                    WHEN tipo_tecnologia =3 THEN 'ETHERNET'
                    WHEN tipo_tecnologia =4 THEN 'DIAL_UP'
                    ELSE (CASE WHEN SUBSTR(A.cprefijo,1, 2) IN (51) THEN 'PINPAD'
                    ELSE 'SIN DATO' END) end AS tipo_tecnologia,
                A.q_tx_wifi,
                A.q_tx_gsm,
                A.q_tx_ethernet,
                A.q_tx_dialup
                
FROM (SELECT DISTINCT SUBSTR(C.cprefijo,1, 2) ||  C.cnumeroterminal    AS nro_terminal,
         D.fecha_instalacion_1                                         AS fecha_instalacion,
         D.resultado_instalacion,
         B.ultima_fecha_actividad,
         B.dias_sin_actividad,
         CASE WHEN D.fecha_instalacion_1 <= DATE_ADD(ultima_fecha_actividad, 15) THEN (dias_sin_actividad / 30.0000)*1.0000 
            ELSE NULL end AS meses_de_inactividad,
         tipo_tecnologia,
         C.cprefijo,
         q_tx_wifi,
         q_tx_gsm,
         q_tx_ethernet,
         q_tx_dialup
      FROM lapos_terminal AS C
      INNER JOIN GPO_ACT_Parque_Instalado_Aux_1_1 AS D ON C.id_terminal=D.terminalid
      LEFT JOIN GPO_ACT_Parque_Instalado_Aux_1_2 AS B ON B.nro_terminal=(SUBSTR(C.cprefijo,1, 2) || C.cnumeroterminal)
      WHERE SUBSTR(C.cprefijo,1, 2) IN ('07', '09','10','11','12','17','18','31','32','33','34','35','36','47','48','49','13','14','15','16', '51') --> Lapos (51 pin pad lapos)
) AS A
"""

Parque_Instalado_Aux_2_1 = """
SELECT id_terminal,
       ccuit,
       nro_establecimiento_contrato,
       nro_establecimiento_visa,
       fecha_alta_estab_visa,
       nro_establecimiento_amex,
       fecha_alta_estab_amex,
       nro_establecimiento_masterfd,
       fecha_alta_estab_masterfd,
       nro_establecimiento_masterprisma,
       fecha_alta_estab_masterprisma,
       nro_establecimiento_cabalpura,
       NULL                                     AS fecha_alta_estab_cabalpura,
       nro_establecimiento_cabalprisma,
       fecha_alta_estab_cabalprisma,
       nro_establecimiento_naranja,
       fecha_alta_estab_naranja,
       nro_establecimiento_naranjavisa,
       fecha_alta_estab_naranjavisa,
       fecha_inicializacion,
       plantilla,
       nro_terminal,
       fecha_instalacion,
       fechacontrato,
       telefono_ater,
       clocalidad,
       vendedor,
       serv_tecnico,
       cod_postal,
       provincia,
       estadoterminal_id,
       estado_terminal,
       superestado_id,
       superestado_terminal,
       codigo_plan,
       cdomiciliocalle,
       empresa_vendedora,
       fecha_proceso
FROM (
    SELECT DISTINCT
            ter.id_terminal,
            contrato.ccuit,
            contrato.cnumeroestablecimiento      AS nro_establecimiento_contrato,
            NULL                                 AS nro_establecimiento_visa,
            NULL                                 AS fecha_alta_estab_visa,
            NULL                                 AS nro_establecimiento_amex,
            NULL                                 AS fecha_alta_estab_amex,
            NULL                                 AS nro_establecimiento_masterfd,
            NULL                                 AS fecha_alta_estab_masterfd,
            NULL                                 AS nro_establecimiento_masterprisma,
            NULL                                 AS fecha_alta_estab_masterprisma,
            NULL                                 AS nro_establecimiento_cabalpura,
            NULL                                 AS fecha_alta_estab_cabalpura,
            NULL                                 AS nro_establecimiento_cabalprisma,
            NULL                                 AS fecha_alta_estab_cabalprisma,
            NULL                                 AS nro_establecimiento_naranja,
            NULL                                 AS fecha_alta_estab_naranja,
            NULL                                 AS nro_establecimiento_naranjavisa,
            NULL                                 AS fecha_alta_estab_naranjavisa,
            NULL                                 AS fecha_inicializacion,
            NULL                                 AS plantilla,
            SUBSTR(ter.cprefijo,1, 2) ||  ter.cnumeroterminal AS nro_terminal,
            t2.dfechalogging                     AS fecha_instalacion,
            contrato.dfechacontrato              AS fechacontrato,
            contrato.ccontactotelefono           AS telefono_ater,
            NULL                                 AS clocalidad,--  COALESCE(cp.detallelocalidad, contrato.cLocalidad) AS clocalidad, --cp.detallelocalidad,--contrato.cLocalidad, 
            NULL                                 AS vendedor,
            NULL                                 AS serv_tecnico,
            NULL                                 AS cod_postal,
            NULL                                 AS provincia,
            ter.estadoterminal_id,
            NULL                                 AS estado_terminal,
            NULL                                 AS superestado_id,
            NULL                                 AS superestado_terminal,
            NULL                                 AS codigo_plan,
            contrato.cdomiciliocalle,
            NULL                                 AS empresa_vendedora,
            '{}' AS fecha_proceso,
            row_number() OVER (PARTITION BY terminalid ORDER BY dfechalogging DESC) rank
    FROM lapos_terminal ter
    LEFT JOIN lapos_contratoestablecimiento contrato ON ter.contratoestablecimiento_id=contrato.id_contratoestablecimiento
    LEFT JOIN lapos_loguser t2 ON ter.id_terminal = t2.terminalid AND t2.postestadoid IN (2,3,11,12,17)
    WHERE SUBSTR(ter.cprefijo,1, 2) IN ('07', '09','10','11','12','17','18','31','32','33','34','35','36','47','48','49','13','14','15','16', '51') --> Lapos   (51 pin pad lapos)
    --AND EstadoTerminal_ID IN (2,3,11,12,17)
) AS A
WHERE rank = 1
"""

Parque_Instalado_Aux_Establecimiento_Query = """
SELECT DISTINCT cuit_establecimiento_host, beneficiario_host FROM (
SELECT cuit_establecimiento_host, beneficiario_host, row_number() OVER (
    PARTITION BY cuit_establecimiento_host ORDER BY fecha_alta DESC
) rank FROM establecimiento_hist
) tmp where rank = 1
"""

Parque_Instalado_Aux_Lapos_ContratoEStablecimiento_Query ="""
SELECT DISTINCT ccuit, crazonsocial FROM (
SELECT ccuit, crazonsocial, row_number() OVER (
    PARTITION BY ccuit ORDER BY dfechacontrato DESC
) rank FROM lapos_contratoestablecimiento
) tmp WHERE rank = 1
"""

Parque_Instalado_Aux_3_Query = """
SELECT A.nro_terminal,
       B.id_terminal,
       B.fecha_instalacion,
       B.fechacontrato                                                             AS fecha_contrato,
       A.resultado_instalacion,
       A.ultima_fecha_actividad,
       A.tipo_inactividad,
       C.cuit_establecimiento_host,
       C.fecha_alta,
       B.nro_establecimiento_visa,
       B.fecha_alta_estab_visa,
       B.nro_establecimiento_amex,
       B.fecha_alta_estab_amex,
       B.nro_establecimiento_masterfd,
       B.fecha_Alta_estab_masterfd,
       B.nro_establecimiento_masterprisma,
       B.fecha_alta_estab_masterprisma,
       B.nro_establecimiento_cabalpura,
       B.fecha_alta_estab_cabalpura,
       B.nro_establecimiento_cabalprisma,
       B.fecha_alta_estab_cabalprisma,
       B.nro_establecimiento_naranja,
       B.fecha_alta_estab_naranja,
       B.nro_establecimiento_naranjavisa,
       B.fecha_alta_estab_naranjavisa,
       B.cdomiciliocalle,
       B.serv_tecnico                                                              AS serv_tecnico,
       B.ccuit                                                                     AS cuit,
       B.nro_establecimiento_contrato,
       B.fecha_inicializacion,
       B.plantilla,
       C.cod_rubro                                                                 AS cod_rubro,
       B.provincia                                                                 AS provincia,
       B.clocalidad                                                                AS localidad,
       B.cod_postal,
       A.tipo_tecnologia,
       A.q_tx_wifi,
       A.q_tx_gsm,
       A.q_tx_ethernet,
       A.q_tx_dialup,
       B.vendedor,
       B.empresa_vendedora,
       B.estadoterminal_id,
       B.estado_terminal,
       B.superestado_id,
       B.superestado_terminal,
       C.nombre_fantasia                                                           AS denominacion,
       COALESCE(COALESCE (C.beneficiario_host,y.beneficiario_host),z.crazonsocial) AS razon_social,
       C.cod_banco                                                                 AS banco_visa,
       CASE WHEN D.nro_cuit IS NOT NULL THEN 'SI' ELSE 'NO' END                       AS cuit_vip,
       CASE WHEN cod_banco IN (16,67,150,415,27,314,316,322,499) THEN 'NO' 
           WHEN cod_banco IS NULL THEN 'NO'
           ELSE 'SI' END                                                           AS acepta_liquidacion,
           B.codigo_plan,
           
       E.codigo_cargo                                                                    AS plan,
       E.nro_mes_cargo                                                                    AS mes_facturacion,
       E.fecha_alta_terminal                                                                 AS fecha_inicio_facturacion,
       E.periodo_inicio_cargo                                                                    AS primer_periodo_facturacion,
       E.periodo                                                                   AS ult_periodo_facturado,
       H.ddn||'-'||H.numero_telefono                                               AS telefono_host,
       B.telefono_ater                                                             AS telefono_ater
FROM GPO_ACT_Parque_Instalado_Aux_1                                  AS A
LEFT JOIN GPO_ACT_Parque_Instalado_Aux_2                             AS B ON A.nro_terminal=B.nro_terminal
LEFT JOIN establecimiento_hist                                       AS C ON B.nro_establecimiento_visa=C.nro_establecimiento
LEFT JOIN lapos_terminalesvip                                        AS D ON D.nro_cuit=B.ccuit
LEFT JOIN lapos_facturacion_terminal                                 AS E ON A.nro_terminal=E.Nro_Terminal 
LEFT JOIN telefono_establecimiento                                   AS H ON B.nro_establecimiento_visa=H.nro_establecimiento
LEFT JOIN GPO_ACT_Parque_Instalado_Aux_Establecimiento               AS Y ON B.ccuit=Y.cuit_establecimiento_host
LEFT JOIN GPO_ACT_Parque_Instalado_Aux_Lapos_ContratoEStablecimiento AS Z ON B.ccuit=Z.ccuit
"""

Parque_Instalado_Query = """
SELECT A.*,
B.fecha_parametrizacion AS ultima_parametrizacion,
C.modelo,
B.plantilla_origen,
B.plantilla_destino,
B.version_origen,
B.medio,
B.marca,
B.plantilla_adecuada,
CASE WHEN plantilla_adecuada='Cambiar Equipo' THEN 'Cambiar Equipo'
        WHEN plantilla_adecuada='Sin modelo' THEN 'Sin Dato'
        WHEN plantilla_adecuada IS NULL THEN 'Sin Dato'
        WHEN plantilla_adecuada<>B.plantilla_destino THEN 'Cambiar Soft'
        WHEN plantilla_adecuada=B.plantilla_destino THEN 'Soft Correcto'
        ELSE 'Mal' END AS condicion_soft
FROM GPO_ACT_Parque_Instalado_Aux_3  AS A
LEFT JOIN lapos_plantilla_adecuada    AS B ON A.nro_terminal=B.id_terminal
LEFT JOIN lapos_terminal_modelo       AS C ON A.nro_terminal=C.id_terminal
"""

mes_Target = """
SELECT nro_terminal,
       nro_establecimiento_orig,
       fecha_host_entrada_autoriz,
       volumen as suma_monto
FROM GPO_sumarizada_autorizacion as A
WHERE CAST(fecha_host_entrada_autoriz as date) between date '{}' and date '{}'
"""

Target_aux_1_query = """
SELECT 

A.cuit_establecimiento_host,
A.nro_establecimiento,

--case when(sum(D.suma_monto) is null or sum(D.suma_monto)/min(J.indice)<85000 or sum(D.suma_monto)/min(J.indice)>500000000) then 0 else 1 end as nivel_activacion,
--case when(sum(D.suma_monto) is null or sum(D.suma_monto)/min(J.indice)>500000000) then 0 else (sum(D.suma_monto)/last(J.indice)) end as monto_max
case when (min(J.indice) > 0.0) then
(case when(sum(D.suma_monto) is null or sum(D.suma_monto)*min(J.indice)<85000 or sum(D.suma_monto)*min(J.indice)>500000000) then 0 else 1 end)
else
(case when(sum(D.suma_monto) is null or sum(D.suma_monto)*1.0<85000 or sum(D.suma_monto)*1.0>500000000) then 0 else 1 end) end
as nivel_activacion,


case when
(min(J.indice) > 0.0) then (sum(D.suma_monto)*min(J.indice)) 
else (sum(D.suma_monto)*1.0)
end as monto_max



FROM GPO_AT2_Establecimientos_Campos AS A
LEFT JOIN GPO_ACT_Parque_Instalado AS B on B.cuit=A.cuit_establecimiento_host AND A.nro_establecimiento = B.nro_establecimiento_contrato
LEFT join at_inflacion_202006 J on J.periodo = extract (year from B.fecha_instalacion)*100 + extract (month from B.fecha_instalacion)
LEFT JOIN mes_sumarizada_target AS D on A.nro_establecimiento = D.nro_establecimiento_orig

GROUP BY 1, 2
"""

Target_aux_2_query = """
SELECT cuit_establecimiento_host,
       max(nivel_activacion) as clase,
       max(monto_max) as monto_terminal
FROM Target_aux_1
GROUP BY 1
"""

Target_query = """
SELECT cuit_establecimiento_host,
       clase,
       monto_terminal
FROM Target_aux_2
"""


if __name__ == "__main__":
    logger.info("Starting transformation.")

    # ---- create argparse and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_bucket", type=str)
    parser.add_argument("--input_prefix", type=str)
    parser.add_argument("--output_bucket", type=str)
    parser.add_argument("--output_prefix", type=str)
    args = parser.parse_args()

    # add info
    logger.info("Received arguments {}".format(args))
    bucket_name = args.input_bucket
    prefix_name = args.input_prefix
    output_bucket = args.output_bucket
    output_prefix = args.output_prefix
    date_ref_str = date_reference = (date.today()).strftime('%Y-%m-%d')

    # create spark session and spark context
    # spark = SparkSession.builder.config("spark.driver.memory", "10g").appName("fg-activterm-target-partition-test-m").getOrCreate()
    # spark.conf.set("spark.sql.shuffle.partitions", 400)
    # spark.conf.set("spark.executor.instances", 10)
    # spark.conf.set("spark.executor.memory", "10g")
    # spark.conf.set("spark.sql.crossJoin.enabled", "true")
    # spark_context = spark.sparkContext

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
    firstday = firstday_tmp - relativedelta(months=12)
    #firstday = date(2019, 1, 1)
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
        "autorizacion": {
            "ssm_param_name": "S3PATH_AUTORIZACION_TABLE",
            "filter_query": autorizacion_filter_query.format(lastyear, lastday),
        }, 
        "estado_establecimiento": {
            "ssm_param_name": "S3PATH_ESTADO_ESTABLECIMIENTO_TABLE",
            "filter_query": None,
        },
        "lapos_loguser": {
            "ssm_param_name": "S3PATH_LAPOS_LOGUSER_TABLE",
            "filter_query": None,
        },
        "lapos_terminal": {
            "ssm_param_name": "S3PATH_LAPOS_TERMINAL_TABLE",
            "filter_query": None,
        },
        "lapos_terminalesvip": {
            "ssm_param_name": "S3PATH_LAPOS_TERMINALESVIP_TABLE",
            "filter_query": None,
        },
        "lapos_contratoestablecimiento": {
            "ssm_param_name": "S3PATH_LAPOS_CONTRATOESTABL_TABLE",
            "filter_query": None,
        },
        "establecimiento_hist": {
            "ssm_param_name": "S3PATH_ESTABLECIMIENTO_HIST_TABLE",
            "filter_query": establ_hist_filter_query.format(lastday, lastyear),
        },
        "lapos_facthist": {
            "ssm_param_name": "S3PATH_LAPOS_FACTHIST_TABLE",
            "filter_query": None,
        },
        "telefono_establecimiento": {
            "ssm_param_name": "S3PATH_TELEFONO_ESTABL_TABLE",
            "filter_query": None,
        },
        "lapos_facturacion_terminal": {
            "ssm_param_name": "S3PATH_LAPOS_FACTURACION_TERMINAL_TABLE",
            "filter_query": None,
        },
        "lapos_plantilla_adecuada": {
            "ssm_param_name": "S3PATH_LAPOS_PLANTILLA_ADECUADA_TABLE",
            "filter_query": None,
        },
        "lapos_terminal_modelo": {
            "ssm_param_name": "S3PATH_LAPOS_TERMINAL_MODELO_TABLE",
            "filter_query": None,
        },
        "at_inflacion_202006": {
            "ssm_param_name": "S3PATH_INFLACION_TABLE",
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

    # ----- create `GPO_AT2_Cuenta_sumarizada_mp` view
    # execute query
    establ_sum_sdf = spark.sql(establ_sum_fmt_query.format(firstday, lastday))

    # add cod_mes column
    establ_sum_sdf = establ_sum_sdf.withColumn(
        "cod_mes", (year(col("fecha_host_entrada_autoriz")) * 100) + (
            month(col("fecha_host_entrada_autoriz"))
        )
    )

    # register temporary view
    establ_sum_sdf.createOrReplaceTempView(
        "GPO_sumarizada_autorizacion"
    )

    ######################################################################################
    logger.info("Creating GPO_AT2_Establecimientos_Campos_temp view.")
    logger.info("Creating GPO_AT2_Establecimientos_Campos view.")

    # execute query
    establ_Prin_Campos_sdf = spark.sql(establ_Prin_Campos_Query.format(lastday, firstday))

    # register temporary view
    establ_Prin_Campos_sdf.createOrReplaceTempView(
        "GPO_AT2_universo_Establecimiento_1_tmp"
    )

    # apply qualify partition over nro_establ
    establ_univ_sdf = spark.sql(establ_Prin_Campos_qualify_query)

    # register temporary view of the final table
    establ_univ_sdf.createOrReplaceTempView(
        "GPO_AT2_Establecimientos_Campos"
    )

    ######################################################################################
    logger.info("Creating GPO_ACT_Parque_Instalado_Aux_1_1 view.")

    # execute query
    Parque_Instalado_Aux_1_1_sdf = spark.sql(Parque_Instalado_Aux_1_1)

    # register temporary view
    Parque_Instalado_Aux_1_1_sdf.createOrReplaceTempView(
        "GPO_ACT_Parque_Instalado_Aux_1_1"
    )

    ######################################################################################    
    logger.info("Creating GPO_ACT_Parque_Instalado_Aux_1_2 view.")

    # execute query
    Parque_Instalado_Aux_1_2_sdf = spark.sql(Parque_Instalado_Aux_1_2.format(lastday))

    # register temporary view
    Parque_Instalado_Aux_1_2_sdf.createOrReplaceTempView(
        "GPO_ACT_Parque_Instalado_Aux_1_2"
    )

    ######################################################################################
    logger.info("Creating GPO_ACT_Parque_Instalado_Aux_1 view.")

    # execute query
    Parque_Instalado_Aux_1_sdf = spark.sql(Parque_Instalado_Aux_1_Query.format(lastday))

    # register temporary view
    Parque_Instalado_Aux_1_sdf.createOrReplaceTempView(
        "GPO_ACT_Parque_Instalado_Aux_1"
    )

    ######################################################################################
    logger.info("Creating GPO_ACT_Parque_Instalado_Aux_2 view.")

    # execute query
    Parque_Instalado_Aux_2_1_sdf = spark.sql(Parque_Instalado_Aux_2_1.format(lastday))

    # register temporary view
    Parque_Instalado_Aux_2_1_sdf.createOrReplaceTempView(
        "GPO_ACT_Parque_Instalado_Aux_2"
    )

    ######################################################################################
    logger.info("Creating GPO_ACT_Parque_Instalado_Aux_Establecimiento view.")

    # execute query
    Parque_Instalado_Aux_Establecimiento_sdf = spark.sql(Parque_Instalado_Aux_Establecimiento_Query)

    # register temporary view
    Parque_Instalado_Aux_Establecimiento_sdf.createOrReplaceTempView(
        "GPO_ACT_Parque_Instalado_Aux_Establecimiento"
    )

    ######################################################################################
    logger.info("Creating GPO_ACT_Parque_Instalado_Aux_Lapos_ContratoEStablecimiento view.")
    # execute query
    Parque_Instalado_Aux_Lapos_ContratoEStablecimiento_sdf = spark.sql(Parque_Instalado_Aux_Lapos_ContratoEStablecimiento_Query)

    # register temporary view
    Parque_Instalado_Aux_Lapos_ContratoEStablecimiento_sdf.createOrReplaceTempView(
        "GPO_ACT_Parque_Instalado_Aux_Lapos_ContratoEStablecimiento"
    )

    ######################################################################################
    logger.info("Creating GPO_ACT_Parque_Instalado_Aux_3_temp view.")
    # execute query
    Parque_Instalado_Aux_3_sdf = spark.sql(Parque_Instalado_Aux_3_Query)

    # register temporary view
    Parque_Instalado_Aux_3_sdf.createOrReplaceTempView(
        "GPO_ACT_Parque_Instalado_Aux_3"
    )

    ######################################################################################
    logger.info("Creating GPO_ACT_Parque_Instalado view.")
    # execute query
    Parque_Instalado_sdf = spark.sql(Parque_Instalado_Query)

    # register temporary view
    Parque_Instalado_sdf.createOrReplaceTempView(
        "GPO_ACT_Parque_Instalado"
    )
    
    ######################################################################################
    logger.info("Creating mes sumarizada Target view.")
    # define dates for target
    dia_inicio_target = lastday.replace(day=1)
    dia_fin_target = lastday
    logger.info("dia_inicio_target {}".format(dia_inicio_target))
    logger.info("dia_fin_target {}".format(dia_fin_target))

    # execute query
    Mes_Target_sdf = spark.sql(mes_Target.format(dia_inicio_target, dia_fin_target))

    # register temporary view
    Mes_Target_sdf.createOrReplaceTempView(
        "mes_sumarizada_target"
    )
    
    ######################################################################################
    logger.info("Creating Target temp 1 view.")
    # execute query
    Target_aux_1_sdf = spark.sql(Target_aux_1_query)

    # register temporary view
    Target_aux_1_sdf.createOrReplaceTempView(
        "Target_aux_1"
    )
    
    ######################################################################################
    logger.info("Creating Target temp 2 view.")
    # execute query
    Target_aux_2_sdf = spark.sql(Target_aux_2_query)

    # register temporary view
    Target_aux_2_sdf.createOrReplaceTempView(
        "Target_aux_2"
    )

    ######################################################################################
    logger.info("Creating Target view.")
    # execute query
    Target_sdf = spark.sql(Target_query)
    Target_sdf = Target_sdf.withColumn("fe_proceso", current_date())
    Target_sdf = Target_sdf.withColumn("id_periodo_analisis", date_format(add_months(to_date(col("fe_proceso"),"yyyy-MM"),-1),"yyyy-MM"))  

    # register temporary view
    Target_sdf.createOrReplaceTempView(
        "Target"
    )

    # ----- transform spark dataframe to pandas and write to parquet
    logger.info("Writing Target to parquet.")
    # repartition dataframe
    Target_sdf = Target_sdf.repartition(total_cores * 3)

    # write to output
    output_path = "s3://" + os.path.join(
        output_bucket, output_prefix, "target_at2_dataset.snappy.parquet")
    Target_sdf.write.mode('overwrite').parquet(output_path, compression='snappy')
