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


bt_adq_autorizacion_establecimiento_v_query = """
select 
A.fecha_host_entrada_autoriz,
EXTRACT(YEAR FROM A.fecha_host_entrada_autoriz)*100+EXTRACT(MONTH FROM A.fecha_host_entrada_autoriz) Cod_Mes_Entrada_Autoriz,
Eh.Nro_Establecimiento, 
SUM(CASE WHEN (A.Cod_Respuesta_Autorizacion IN (0, 1, 4, 7, 400)  OR  (A.Cod_Respuesta_Autorizacion = 900 AND TRIM(A.Nro_Autorizacion) <> '')) THEN 1 ELSE 0 END) AS Cantidad_Transacciones,

SUM(CASE WHEN (A.Cod_Respuesta_Autorizacion IN (0, 1, 4, 7, 400)  OR  (A.Cod_Respuesta_Autorizacion = 900 AND TRIM(A.Nro_Autorizacion) <> '')) THEN A.Monto_Autoriz_Moneda_Orig ELSE 0 END) AS Volumen_Transacciones

from autorizacion as A
INNER JOIN ESTABLECIMIENTO_HIST Eh
 ON Eh.Nro_Establecimiento = A.Nro_Establecimiento_Orig
 --AND eh.fecha_inicio <= A.Fecha_Host_Entrada_Autoriz
 --AND (eh.fecha_fin >= A.Fecha_Host_Entrada_Autoriz OR (CAST(Eh.fecha_fin AS date) = date '9999-12-31') )
 AND A.Cod_Tipo_Mensaje_Autoriz = 1100
 AND (A.Programa_Periferia_Origen BETWEEN 19000 AND 19999 --DCH:Debito automatico
 OR (A.Programa_Periferia_Origen BETWEEN 520000 AND 549999)) ---> Adquirencia Prisma:

AND A.Nro_Terminal NOT IN ('17010499') -->Saco terminal prueba.
AND  (A.Cod_Respuesta_Autorizacion IN (0, 1, 4, 7, 400)  OR  (A.Cod_Respuesta_Autorizacion = 900 AND TRIM(A.Nro_Autorizacion) <> '')) -- Aprobadas 


group by 1,2,3
""" ## lastyear and lastday



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
     , id_carga
FROM direccion_establecimiento_hist 
WHERE CAST(fecha_fin as date) = date '9999-12-31' 
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
WHERE (CAST(fecha_inicio AS date) <= date '{}') AND ( (CAST(fecha_fin AS date) > date '{}') OR (CAST(fecha_fin AS date) = date '9999-12-31') )
""" 

terminal_hist_filter_query = """
SELECT fecha_fin
     , fecha_inicio
     , nro_establecimiento
     , nro_terminal
FROM terminal_hist
WHERE (fecha_inicio <= date '{}') AND ((fecha_fin > date '{}') OR (fecha_fin IS NULL))
""" ## lastday, lastyear

mov_present_filter_query = """
SELECT fecha_movimiento
     , nro_establecimiento
     , cod_tipo_operacion
     , monto_movimiento
     , monto_movimiento_origen
FROM movimiento_presentado 
WHERE fecha_movimiento BETWEEN '{}' AND '{}'
""" ## lastyear, lastday

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
       e.cod_rubro,
       R.desc_rubro,
       nombre_fantasia,
       beneficiario_host,
       e.cod_estado_establecimiento,
       EE.ds_estado_establecimiento       AS desc_estado_establecimiento,
       trim(nivel_seguridad_moto_ecom)    AS nivel_seguridad_moto_ecom,
       bandera,
       e.id_carga
FROM establecimiento_hist e 
LEFT JOIN estado_establecimiento EE on e.cod_estado_establecimiento = EE.cd_estado_establecimiento
LEFT JOIN rubro as r on e.cod_rubro = r.cod_rubro
"""

establ_Prin_Campos_qualify_query = """
SELECT cuit_establecimiento_host,
       nro_establecimiento,
       cod_rubro,
       desc_rubro,
       nombre_fantasia,
       beneficiario_host,
       cod_estado_establecimiento,
       desc_estado_establecimiento,
       nivel_seguridad_moto_ecom,
       bandera
FROM (
    SELECT cuit_establecimiento_host,
          nro_establecimiento,
          cod_rubro,
          desc_rubro,
          nombre_fantasia,
          beneficiario_host,
          cod_estado_establecimiento,
          desc_estado_establecimiento,
          nivel_seguridad_moto_ecom,
          bandera,
          row_number() OVER (PARTITION BY nro_establecimiento ORDER BY id_carga DESC) AS rank 
    FROM GPO_AT2_universo_Establecimiento_1_tmp
) AS tmp 
WHERE rank = 1
"""

# Listar Terminales
Terminales_Campos_Query = """
SELECT DISTINCT 
    T.nro_establecimiento,
    T.nro_terminal

FROM terminal_hist T

WHERE SUBSTR(T.nro_terminal,1,2) IN ('07','08','09','10','11','12','17','18','31','32','33','34','35','36','47','48','49','13','14','15','16','51') 
"""

# Listar Direcciones Establecimiento
Direcciones_Campos_Aux_Query = """
SELECT 
    nro_establecimiento,
    H.cod_provincia,
    H.desc_provincia,
    COALESCE(G.calle,'')       AS nombre_calle,
    COALESCE(G.numero,'')      AS numero_calle,
    G.codigo_postal4           AS codigo_postal,
    G.cod_localidad,
    G.nombre_localidad         AS localidad,
    CASE WHEN COALESCE(G.calle,'') = '' OR COALESCE(G.calle,'') IS NULL THEN '' ELSE COALESCE(G.calle,'') || ', ' END ||
    CASE WHEN COALESCE(G.numero,'') = '' OR COALESCE(G.numero,'') IS NULL THEN '' ELSE COALESCE(G.numero,'') || ', ' END ||
        CASE WHEN G.nombre_localidad = '' OR G.nombre_localidad IS NULL THEN '' ELSE G.nombre_localidad || ', ' END ||
            CASE WHEN H.desc_provincia = '' OR H.desc_provincia IS NULL THEN '' ELSE H.desc_provincia || ', ' END ||
                'ARGENTINA' AS direccion,
    G.id_carga

FROM direccion_establecimiento_hist AS G
LEFT JOIN provincia AS H ON H.cod_provincia=SUBSTR(G.cod_region_geografica_host,1,1)

WHERE G.cod_tipo_direccion= '{}'
"""


Direcciones_Campos_qualify_query = """
SELECT nro_establecimiento,
       cod_provincia,
       desc_provincia,
       nombre_calle,
       numero_calle,
       numero_calle,
       codigo_postal,
       cod_localidad,
       localidad,
       direccion 
FROM (
    SELECT nro_establecimiento,
           cod_provincia,
           desc_provincia,
           nombre_calle,
           numero_calle,
           numero_calle,
           codigo_postal,
           cod_localidad,
           localidad,
           direccion,
           row_number() OVER (PARTITION BY nro_establecimiento ORDER BY id_carga DESC) rank 
    FROM direccion{}_temp) tmp

WHERE rank = 1
"""

Direcciones_Campos_query = """
SELECT BASE.nro_establecimiento,
       CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.nombre_calle else A.nombre_calle end nombre_calle,
       CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.numero_calle else A.numero_calle end numero_calle,
       CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.codigo_postal else A.codigo_postal end codigo_postal,
       CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.cod_localidad else A.cod_localidad end cod_localidad,
       CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.localidad else A.localidad end localidad,
       CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.cod_provincia else A.cod_provincia end cod_provincia,
       CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.desc_provincia else A.desc_provincia end desc_provincia,
       CASE WHEN A.nombre_calle is null or A.nombre_calle = '' then B.direccion else A.direccion end direccion

FROM GPO_AT2_Establecimientos_Campos as BASE
LEFT JOIN direccion2 as A on base.nro_establecimiento = A.nro_establecimiento
LEFT JOIN direccion1 as B on base.nro_establecimiento = B.nro_establecimiento
"""

# definicion del universo
establ_Prin_Univ_aux_1_Query = """
SELECT DISTINCT E.nro_establecimiento,
                E.cuit_establecimiento_host,
                T.nro_terminal,
                E.cod_rubro,
                E.desc_rubro,
                E.nombre_fantasia,
                E.beneficiario_host,
                E.cod_estado_establecimiento,
                E.desc_estado_establecimiento,
                E.nivel_seguridad_moto_ecom,
                E.bandera,
                CASE WHEN G.desc_provincia='BUENOS AIRES' AND G2.desc_departamento_geo like '%Comuna%' THEN 0 ELSE G2.latitud END latitud,
                CASE WHEN G.desc_provincia='BUENOS AIRES' AND G2.desc_departamento_geo like '%Comuna%' THEN 0 ELSE G2.longitud END longitud,
                CASE WHEN G.desc_provincia='BUENOS AIRES' AND G2.desc_departamento_geo like '%Comuna%' THEN NULL ELSE G2.desc_departamento_geo END desc_departamento_geo,
                G2.desc_provincia_geo,
                G2.desc_localidad_gba_geo,
                G.cod_provincia,
                G.desc_provincia,
                G.nombre_calle,
                G.numero_calle,
                G.codigo_postal,
                G.cod_localidad,
                G.localidad,
                G.direccion

FROM GPO_AT2_Establecimientos_Campos as E
INNER JOIN GPO_AT2_Terminales_Campos as T on T.nro_establecimiento=E.nro_establecimiento
LEFT JOIN GPO_AT2_Direcciones_Campos as G on G.nro_establecimiento=E.nro_establecimiento
LEFT JOIN direccion_establecimiento_geolocalizado_ult_inst G2 on G2.nro_establecimiento=E.nro_establecimiento
"""

establ_Prin_Univ_aux_2_Query = """
SELECT DISTINCT T.nro_establecimiento,
                T.nro_terminal

FROM terminal_hist T

WHERE SUBSTR(T.nro_terminal,1,2) IN ('07','08','09','10','11','12','17','18','31','32','33','34','35','36','47','48','49','13','14','15','16','51') 
"""

# Colocar nombre de query
establ_Prin_Univ_aux_3_Query = """
SELECT DISTINCT E.nro_establecimiento,
                E.cuit_establecimiento_host,
                T.nro_terminal

FROM GPO_AT2_Establecimientos_Campos E
INNER JOIN GPO_AT2_universo_inicial_ref2_aux T on T.nro_establecimiento=e.nro_establecimiento
INNER JOIN bt_adq_autorizacion_establecimiento_v A on A.nro_establecimiento=e.nro_establecimiento and A.fecha_host_entrada_autoriz between date '{}' and date '{}'
"""

establ_Prin_Univ_Query = """
SELECT e_ref2.nro_establecimiento,
       e_ref.nro_establecimiento                AS nro_establecimiento_ref,
       e_ref.cuit_establecimiento_host          AS cuit_establecimiento_host_ref,
       e_ref.nro_terminal nro_terminal,
       e_ref.cod_rubro,
       e_ref.desc_rubro,
       e_ref.nombre_fantasia,
       e_ref.beneficiario_host,
       e_ref.cod_estado_establecimiento,
       e_ref.desc_estado_establecimiento,
       e_ref.nivel_seguridad_moto_ecom,
       e_ref.bandera,
       e_ref.latitud,
       e_ref.longitud,
       e_ref.desc_departamento_geo,
       e_ref.desc_provincia_geo,
       e_ref.desc_localidad_gba_geo,
       e_ref.cod_provincia,
       e_ref.desc_provincia,
       e_ref.nombre_calle,
       e_ref.numero_calle,
       e_ref.codigo_postal,
       e_ref.cod_localidad,
       e_ref.localidad, 
       e_ref.direccion

FROM GPO_AT2_universo_inicial_ref e_ref
INNER JOIN GPO_AT2_universo_inicial_ref2 e_ref2 ON e_ref.cuit_establecimiento_host=e_ref2.cuit_establecimiento_host AND e_ref.nro_terminal=e_ref2.nro_terminal

UNION ALL

SELECT e_ref.nro_establecimiento,
       e_ref.nro_establecimiento                AS nro_establecimiento_ref,
       e_ref.cuit_establecimiento_host          AS cuit_establecimiento_host_ref,
       e_ref.nro_terminal nro_terminal_ref,
       e_ref.cod_rubro,
       e_ref.desc_rubro,
       e_ref.nombre_fantasia,
       e_ref.beneficiario_host,
       e_ref.cod_estado_establecimiento,
       e_ref.desc_estado_establecimiento,
       e_ref.nivel_seguridad_moto_ecom,
       e_ref.bandera,
       e_ref.latitud,
       e_ref.longitud,
       e_ref.desc_departamento_geo,
       e_ref.desc_provincia_geo,
       e_ref.desc_localidad_gba_geo,
       e_ref.cod_provincia,
       e_ref.desc_provincia,
       e_ref.nombre_calle,
       e_ref.numero_calle,
       e_ref.codigo_postal,
       e_ref.cod_localidad,
       e_ref.localidad, 
       e_ref.direccion
FROM GPO_AT2_universo_inicial_ref e_ref
"""

#ver mismo tema con braces
month_aut_fmt_query = """
SELECT aut.nro_establecimiento,
       SUM(cantidad_transacciones)                     AS cant_m{},
       SUM(volumen_transacciones/coalesce(aut.indice, 1))  AS vol_m{}
FROM (
    SELECT nro_establecimiento,
           cantidad_transacciones,
           volumen_transacciones,
           I.indice
    FROM bt_adq_autorizacion_establecimiento_v A 
    --LEFT JOIN at_inflacion_202006 I on I.periodo= (year("fecha_host_entrada_autoriz") * 100) + (month("fecha_host_entrada_autoriz"))
    LEFT JOIN at_inflacion_202006 I on I.periodo = extract(year from A.fecha_host_entrada_autoriz)*100 + extract(month from A.fecha_host_entrada_autoriz)
    WHERE CAST(fecha_host_entrada_autoriz as date) BETWEEN date '{}' AND date '{}'
) AS aut
GROUP BY nro_establecimiento
"""

cant_term_ref_query = """
SELECT nro_establecimiento,
       COUNT(DISTINCT nro_terminal) AS cant_term

FROM GPO_AT2_universo_inicial_ref3

GROUP BY nro_establecimiento
"""

# Txs_Query -- txs_estab_ref
establ_Prin_Transacciones_Aux_query = """
SELECT T.nro_establecimiento nro_establecimiento_ref,
       cant_term,
       ROUND(cant_m12/cant_term,2) AS cant_m12,
       ROUND(vol_m12/cant_term,2)  AS vol_m12,
       ROUND(cant_m11/cant_term,2) AS cant_m11,
       ROUND(vol_m11/cant_term,2)  AS vol_m11,
       ROUND(cant_m10/cant_term,2) AS cant_m10,
       ROUND(vol_m10/cant_term,2) AS vol_m10,
       ROUND(cant_m9/cant_term,2)  AS cant_m9,
       ROUND(vol_m9/cant_term,2)   AS vol_m9,
       ROUND(cant_m8/cant_term,2)  AS cant_m8,
       ROUND(vol_m8/cant_term,2)   AS vol_m8,
       ROUND(cant_m7/cant_term,2)  AS cant_m7,
       ROUND(vol_m7/cant_term,2)   AS vol_m7,
       ROUND(cant_m6/cant_term,2)  AS cant_m6,
       ROUND(vol_m6/cant_term,2)   AS vol_m6,
       ROUND(cant_m5/cant_term,2)  AS cant_m5,
       ROUND(vol_m5/cant_term,2)   AS vol_m5,
       ROUND(cant_m4/cant_term,2)  AS cant_m4,
       ROUND(vol_m4/cant_term,2)   AS vol_m4,
       ROUND(cant_m3/cant_term,2)  AS cant_m3,
       ROUND(vol_m3/cant_term,2)   AS vol_m3,
       ROUND(cant_m2/cant_term,2)  AS cant_m2,
       ROUND(vol_m2/cant_term,2)   AS vol_m2,
       ROUND(cant_m1/cant_term,2)  AS cant_m1,
       ROUND(vol_m1/cant_term,2)   AS vol_m1

FROM GPO_AT2_universo_inicial_ref3 T 
LEFT JOIN cant_term_ref E2 on E2.nro_establecimiento = T.nro_establecimiento
LEFT JOIN month1_establecimiento_Principal_cant  m1 on T.nro_establecimiento = m1.nro_establecimiento
LEFT JOIN month2_establecimiento_Principal_cant  m2 on T.nro_establecimiento = m2.nro_establecimiento
LEFT JOIN month3_establecimiento_Principal_cant  m3 on T.nro_establecimiento = m3.nro_establecimiento
LEFT JOIN month4_establecimiento_Principal_cant  m4 on T.nro_establecimiento = m4.nro_establecimiento
LEFT JOIN month5_establecimiento_Principal_cant  m5 on T.nro_establecimiento = m5.nro_establecimiento
LEFT JOIN month6_establecimiento_Principal_cant  m6 on T.nro_establecimiento = m6.nro_establecimiento
LEFT JOIN month7_establecimiento_Principal_cant  m7 on T.nro_establecimiento = m7.nro_establecimiento
LEFT JOIN month8_establecimiento_Principal_cant  m8 on T.nro_establecimiento = m8.nro_establecimiento
LEFT JOIN month9_establecimiento_Principal_cant  m9 on T.nro_establecimiento = m9.nro_establecimiento
LEFT JOIN month10_establecimiento_Principal_cant  m10 on T.nro_establecimiento = m10.nro_establecimiento
LEFT JOIN month11_establecimiento_Principal_cant  m11 on T.nro_establecimiento = m11.nro_establecimiento
LEFT JOIN month12_establecimiento_Principal_cant  m12 on T.nro_establecimiento = m12.nro_establecimiento
"""

establ_Prin_Transacciones_query = """
SELECT nro_establecimiento_ref,
       sum(cant_m12) AS cant_txs_m12,
       sum(vol_m12)  AS vol_txs_m12,
       sum(cant_m11) AS cant_txs_m11,
       sum(vol_m11)  AS vol_txs_m11,
       sum(cant_m10) AS cant_txs_m10,
       sum(vol_m10)  AS vol_txs_m10,
       sum(cant_m9)  AS cant_txs_m9,
       sum(vol_m9)   AS vol_txs_m9,
       sum(cant_m8)  AS cant_txs_m8,
       sum(vol_m8)   AS vol_txs_m8,
       sum(cant_m7)  AS cant_txs_m7,
       sum(vol_m7)   AS vol_txs_m7,
       sum(cant_m6)  AS cant_txs_m6,
       sum(vol_m6)   AS vol_txs_m6,
       sum(cant_m5)  AS cant_txs_m5,
       sum(vol_m5)   AS vol_txs_m5,
       sum(cant_m4)  AS cant_txs_m4,
       sum(vol_m4)   AS vol_txs_m4,
       sum(cant_m3)  AS cant_txs_m3,
       sum(vol_m3)   AS vol_txs_m3,
       sum(cant_m2)  AS cant_txs_m2,
       sum(vol_m2)   AS vol_txs_m2,
       sum(cant_m1)  AS cant_txs_m1,
       sum(vol_m1)   AS vol_txs_m1

FROM GPO_AT2_txs_estab_ref

GROUP BY nro_establecimiento_ref"""

Parque_Instalado_Aux_1_1 = """
SELECT terminalid,
       fecha_instalacion_1,
       resultado_instalacion
FROM (
    SELECT terminalid,
           dfechalogging   AS fecha_instalacion_1,
           postestadoid    AS resultado_instalacion,
           row_number() OVER (PARTITION BY terminalid ORDER BY dfechalogging ASC) rank 
    FROM lapos_loguser) tmp 

WHERE rank = 1
"""

Parque_Instalado_Aux_1_2 = """
SELECT 
    nro_terminal,
    MAX(fecha_host_entrada_autoriz)                                               AS ultima_fecha_actividad,
    DATEDIFF('{}', MAX(fecha_host_entrada_autoriz))                               AS dias_sin_actividad,
    MIN(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='W' THEN 1
                        WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='G' THEN 2
                        WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='E' THEN 3
                        WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='D' THEN 4
                        ELSE 9 END)                                               AS tipo_tecnologia, -- Este es el orden de prioridad de las tecnologias
    SUM(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='W' THEN 1 ELSE 0 end)  AS q_tx_wifi,
    SUM(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='G' THEN 1 ELSE 0 end)  AS q_tx_gsm,
    SUM(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='E' THEN 1 ELSE 0 end)  AS q_tx_ethernet,
    SUM(CASE WHEN LEFT(RIGHT(a.datos_adicionales_2 ,5),1)='D' THEN 1 ELSE 0 end)  AS q_tx_dialup

FROM autorizacion AS a

WHERE ( a.cod_respuesta_autorizacion IN (0, 1, 4, 7, 400) OR (a.cod_respuesta_autorizacion = 900 AND TRIM(a.nro_autorizacion) <> ''))
     AND monto_autoriz_moneda_orig >= 300 --Transacciones aprobadas mayor a $300	
     AND  SUBSTR(a.nro_terminal,1, 2) IN ('07', '09','10','11','12','17','18','31','32','33','34','35','36','47','48','49','13','14','15','16', '51') 

GROUP BY nro_terminal
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
SELECT DISTINCT cuit_establecimiento_host, beneficiario_host 

FROM (
    SELECT cuit_establecimiento_host, 
            beneficiario_host, 
            row_number() OVER (
                PARTITION BY cuit_establecimiento_host ORDER BY fecha_alta DESC ) rank FROM establecimiento_hist) tmp 

where rank = 1
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

establ_Prin_Niveles_Activacion_Aux_1_query = """
SELECT 
    U.cuit_establecimiento_host_ref,
    U.nro_establecimiento_ref,
    U.nro_establecimiento,
    T.nro_terminal,
    case when(sum(suma_monto)<85000*coalesce(min(I.indice),0) or sum(suma_monto) is null) then 0 else 1 end as nivel_activacion
FROM GPO_AT2_universo_inicial_ref3_distinct U
INNER JOIN GPO_ACT_Parque_Instalado T on T.cuit=U.cuit_establecimiento_host_ref and cast(T.nro_establecimiento_contrato as decimal(18,0))=U.nro_establecimiento and T.fecha_instalacion between '{}' and '{}'
INNER JOIN at_inflacion_202006 I on I.periodo = extract (year from fecha_instalacion)*100 + extract (month from fecha_instalacion)
LEFT JOIN (
    SELECT 
        nro_terminal,
        fecha_host_entrada_autoriz,
        sum(volumen) suma_monto
    FROM GPO_sumarizada_autorizacion A 
    WHERE CAST(A.fecha_host_entrada_autoriz as date) between date '{}' and date '{}'
    GROUP BY 1,2 ) A ON A.nro_terminal=T.nro_terminal AND A.fecha_host_entrada_autoriz between T.fecha_instalacion and T.fecha_instalacion + interval '30' day
GROUP BY 1,2,3,4
"""

establ_Prin_Niveles_Activacion_Aux_2_query = """
select  
U.nro_establecimiento_ref, 
sum(case when nivel_activacion=0 then 1 else 0 end) terminales_nivel_0,
sum(case when nivel_activacion=1 then 1 else 0 end) terminales_nivel_1
from GPO_AT2_universo_inicial_ref3_distinct U
left join GPO_AT2_niveles_activacion_Establecimiento_ref_aux aux on aux.nro_establecimiento_ref=U.nro_establecimiento_ref
group by 1
"""

establ_Prin_Niveles_Activacion_query = """
SELECT nro_establecimiento_ref,
       avg(terminales_nivel_0) avg_terminales_nivel_0,
       avg(terminales_nivel_1) avg_terminales_nivel_1
from GPO_AT2_niveles_activacion_ref T
group by 1
"""

establ_Prin_Dias_Terminales_Aux_1_query = """
SELECT U.cuit_establecimiento_host_ref,
    U.nro_establecimiento_ref,
    U.nro_establecimiento,
    T.nro_terminal,
    T.fecha_instalacion,
    fecha_host_entrada_autoriz,
    SUM(A.monto_autoriz_moneda_orig) OVER (PARTITION BY T.nro_terminal ORDER BY A.fecha_host_entrada_autoriz ROWS UNBOUNDED PRECEDING) as vol_cum
FROM GPO_AT2_universo_inicial_ref3_distinct U
inner join GPO_ACT_Parque_Instalado T 
    on T.cuit=U.cuit_establecimiento_host_ref and T.fecha_instalacion between '{}' and '{}'
left join (
    select 
        nro_terminal,
        fecha_host_entrada_autoriz,
        sum(volumen) as monto_autoriz_moneda_orig
    from GPO_sumarizada_autorizacion A 
    where CAST(A.fecha_host_entrada_autoriz as date) between date '{}' and date '{}'
    group by 1,2 
) A on A.nro_terminal=T.nro_Terminal and A.fecha_host_entrada_autoriz between T.fecha_instalacion and T.fecha_instalacion + interval '30' day
"""

establ_Prin_Dias_Terminales_Aux_2_query = """
SELECT B.nro_establecimiento_ref,
       coalesce(B.avg_dias_1tx,180)   AS avg_dias_1tx,
       coalesce(B.avg_dias_85000,180) AS avg_dias_85000
FROM (
    SELECT U.nro_establecimiento_ref,
           cod_rubro,
           AVG(dias_1tx)   AS avg_dias_1tx,
           AVG(dias_85000) AS avg_dias_85000
    FROM (
        SELECT DISTINCT cuit_establecimiento_host_ref,
                        nro_establecimiento_ref,
                        cod_rubro
        FROM GPO_AT2_universo_inicial_ref3 ) AS U
    INNER JOIN (
          SELECT cuit_establecimiento_host_ref,
                 nro_establecimiento_ref,
                 nro_establecimiento,
                 nro_terminal,
                 DATEDIFF(MIN(CASE WHEN (vol_cum >= 0) THEN fecha_host_entrada_autoriz ELSE NULL END), MIN(fecha_instalacion)) as dias_1tx,
                 DATEDIFF(MIN(CASE WHEN (vol_cum >= (85000 * I.indice)) THEN fecha_host_entrada_autoriz ELSE NULL END), MIN(fecha_instalacion)) as dias_85000
          FROM GPO_AT2_dias_terminales_Establecimiento_ref_aux A
          INNER JOIN at_inflacion_202006 I on I.periodo = extract (year from fecha_instalacion)*100 + extract (month from fecha_instalacion)
          GROUP BY cuit_establecimiento_host_ref, nro_establecimiento_ref, nro_establecimiento, nro_terminal
    ) AS T ON T.cuit_establecimiento_host_ref=U.cuit_establecimiento_host_ref and T.nro_establecimiento_ref=U.nro_establecimiento_ref
    GROUP BY 1, 2
) AS B
"""

establ_Prin_Dias_Terminales_query = """
SELECT nro_establecimiento_ref,
       AVG(T.avg_dias_1tx)       AS avg_dias_1tx,
       AVG(T.avg_dias_85000)     AS avg_dias_85000
FROM GPO_AT2_dias_terminales_ref AS T 
GROUP BY nro_establecimiento_ref
"""

ABT_Establecimiento_Principal_query = """
SELECT DISTINCT
     CAST(U.cuit_establecimiento_host_ref AS STRING),
     CAST(U.nro_establecimiento_ref AS INT),
     CAST(U.cod_rubro AS STRING),
     CAST(U.desc_rubro AS STRING),
     CAST(U.nombre_fantasia AS STRING),
     CAST(U.beneficiario_host AS STRING),
     CAST(U.cod_estado_establecimiento AS STRING),
     CAST(U.desc_estado_establecimiento AS STRING),
     CAST(U.nivel_seguridad_moto_ecom AS STRING),
     CAST(U.bandera AS INT),
     CAST(U.nombre_calle AS STRING),
     CAST(U.numero_calle AS STRING),
     CAST(U.codigo_postal AS STRING),
     CAST(U.cod_localidad AS INT),
     CAST(U.localidad AS STRING),
     CAST(U.cod_provincia AS STRING),
     CAST(U.desc_provincia AS STRING),
     CAST(U.direccion AS STRING),
     CAST(U.desc_departamento_geo AS STRING),
     CAST(U.desc_provincia_geo AS STRING),
     CAST(U.latitud AS FLOAT),
     CAST(U.longitud AS FLOAT),
     coalesce(CAST(T.cant_txs_m12 AS INT), 0)             AS cant_txs_m12,
     coalesce(CAST(T.vol_txs_m12 AS FLOAT), 0.0)            AS vol_txs_m12,
     coalesce(CAST(T.cant_txs_m11 AS INT), 0)             AS cant_txs_m11,
     coalesce(CAST(T.vol_txs_m11 AS FLOAT), 0.0)            AS vol_txs_m11,
     coalesce(CAST(T.cant_txs_m10 AS INT), 0)             AS cant_txs_m10,
     coalesce(CAST(T.vol_txs_m10 AS FLOAT), 0.0)            AS vol_txs_m10,
     coalesce(CAST(T.cant_txs_m9 AS INT), 0)              AS cant_txs_m9,
     coalesce(CAST(T.vol_txs_m9 AS FLOAT), 0.0)             AS vol_txs_m9,
     coalesce(CAST(T.cant_txs_m8 AS INT), 0)              AS cant_txs_m8,
     coalesce(CAST(T.vol_txs_m8 AS FLOAT), 0.0)             AS vol_txs_m8,
     coalesce(CAST(T.cant_txs_m7 AS INT), 0)              AS cant_txs_m7,
     coalesce(CAST(T.vol_txs_m7 AS FLOAT), 0.0)             AS vol_txs_m7,
     coalesce(CAST(T.cant_txs_m6 AS INT), 0)              AS cant_txs_m6,
     coalesce(CAST(T.vol_txs_m6 AS FLOAT), 0.0)             AS vol_txs_m6,
     coalesce(CAST(T.cant_txs_m5 AS INT), 0)              AS cant_txs_m5,
     coalesce(CAST(T.vol_txs_m5 AS FLOAT), 0.0)             AS vol_txs_m5,
     coalesce(CAST(T.cant_txs_m4 AS INT), 0)              AS cant_txs_m4,
     coalesce(CAST(T.vol_txs_m4 AS FLOAT), 0.0)             AS vol_txs_m4,
     coalesce(CAST(T.cant_txs_m3 AS INT), 0)              AS cant_txs_m3,
     coalesce(CAST(T.vol_txs_m3 AS FLOAT), 0.0)             AS vol_txs_m3,
     coalesce(CAST(T.cant_txs_m2 AS INT), 0)              AS cant_txs_m2,
     coalesce(CAST(T.vol_txs_m2 AS FLOAT), 0.0)             AS vol_txs_m2,
     coalesce(CAST(T.cant_txs_m1 AS INT), 0)              AS cant_txs_m1,
     coalesce(CAST(T.vol_txs_m1 AS FLOAT), 0.0)             AS vol_txs_m1,
     coalesce(CAST(N.avg_terminales_nivel_0 AS FLOAT), 0.0) AS avg_terminales_nivel_0,
     coalesce(CAST(N.avg_terminales_nivel_1 AS FLOAT), 0.0) AS avg_terminales_nivel_1,
     coalesce(CAST(D.avg_dias_1tx AS FLOAT), 0.0)           AS avg_dias_1tx,
     coalesce(CAST(D.avg_dias_85000 AS FLOAT), 0.0)         AS avg_dias_85000

FROM GPO_AT2_universo_inicial_ref3                       AS U
LEFT JOIN GPO_AT2_txs_Establecimiento_Ref                AS T ON T.nro_establecimiento_ref=U.nro_establecimiento_ref
LEFT JOIN GPO_AT2_niveles_activacion_Establecimiento_Ref AS N ON N.nro_establecimiento_ref=U.nro_establecimiento_ref
LEFT JOIN GPO_AT2_dias_terminales_Establecimiento_Ref    AS D ON D.nro_establecimiento_ref=U.nro_establecimiento_ref
"""

if __name__ == "__main__":
    logger.info("Starting transformation.")

    # ---- create argparse and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_bucket", type=str)
    parser.add_argument("--input_prefix", type=str)
    parser.add_argument("--output_bucket", type=str)
    parser.add_argument("--output_prefix", type=str)
    parser.add_argument("--date_reference", type=str)
    args = parser.parse_args()

    bucket_name = args.input_bucket
    prefix_name = args.input_prefix
    output_bucket = args.output_bucket
    output_prefix = args.output_prefix
    date_ref_str = (date.today()).strftime('%Y-%m-%d')
    #date_ref_str = "2021-11-30"

    # create spark session and spark context
    spark = SparkSession.builder.config("spark.driver.memory", "10g").appName("fg-establecimiento-principal").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 400)
    spark.conf.set("spark.executor.instances", 10)
    spark.conf.set("spark.executor.memory", "10g")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
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
#         "bt_adq_autorizacion_establecimiento_v":{
#             "ssm_param_name": "S3PATH_BT_ADQ_AUT_ESTABL_V_TABLE",
#             "filter_query": None,
#         }, 
        "autorizacion": {
            "ssm_param_name": "S3PATH_AUTORIZACION_TABLE",
            "filter_query": autorizacion_filter_query.format(lastyear, lastday),
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
        "lapos_contratoestablecimiento":{
            "ssm_param_name": "S3PATH_LAPOS_CONTRATOESTABL_TABLE",
            "filter_query": None,
        },
        "provincia": {
            "ssm_param_name": "S3PATH_PROVINCIA_TABLE",
            "filter_query": None,
        },
        "rubro": {
            "ssm_param_name": "S3PATH_RUBRO_TABLE",
            "filter_query": None,
        },
        "terminal_hist": {
            "ssm_param_name": "S3PATH_TERMINAL_HIST_TABLE",
            "filter_query": terminal_hist_filter_query.format(lastday, lastyear),
        },
        "telefono_establecimiento": {
            "ssm_param_name": "S3PATH_TELEFONO_ESTABL_TABLE",
            "filter_query": None,
        },
        "lapos_facturacion_terminal": {
            "ssm_param_name": "S3PATH_LAPOS_FACTURACION_TERMINAL_TABLE",
            "filter_query": None,
        },
        "lapos_loguser": {
            "ssm_param_name": "S3PATH_LAPOS_LOGUSER_TABLE",
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
        "lapos_terminal": {
            "ssm_param_name": "S3PATH_LAPOS_TERMINAL_TABLE",
            "filter_query": None,
        },
        "lapos_terminalesvip": {
            "ssm_param_name": "S3PATH_LAPOS_TERMINALESVIP_TABLE",
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
    ######################################################################################
    logger.info("Creating bt_adq_autorizacion_establecimiento_v view.")
    # execute query
    bt_adq_autorizacion_establecimiento_v_sdf = spark.sql(bt_adq_autorizacion_establecimiento_v_query)


    # register temporary view
    bt_adq_autorizacion_establecimiento_v_sdf.createOrReplaceTempView(
        "bt_adq_autorizacion_establecimiento_v"
    )


    ######################################################################################
    logger.info("Creating GPO_AT2_Establecimientos_Campos_temp view.")
    logger.info("Creating GPO_AT2_Establecimientos_Campos view.")

    # execute query
    establ_Prin_Campos_sdf = spark.sql(establ_Prin_Campos_Query)

    # select columns
    establ_Prin_Campos_sdf = establ_Prin_Campos_sdf.select(
        "cuit_establecimiento_host",
        "nro_establecimiento",
        "cod_rubro",
        "desc_rubro",
        "nombre_fantasia",
        "beneficiario_host",
        "cod_estado_establecimiento",
        "desc_estado_establecimiento",
        "nivel_seguridad_moto_ecom",
        "bandera",
        "id_carga"
    )

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
    ######################################################################################
    logger.info("Creating GPO_AT2_Terminales_Campos view.")
    # execute query
    Terminales_Campos_sdf = spark.sql(Terminales_Campos_Query)

    # select columns
    establ_univ_tmp_sdf = Terminales_Campos_sdf.select(
        "nro_establecimiento",
        "nro_terminal"
    )

    # register temporary view
    establ_univ_tmp_sdf.createOrReplaceTempView(
        "GPO_AT2_Terminales_Campos"
    )

    #####################################################################################
    logger.info("Creating direccion1_temp view.")
    logger.info("Creating direccion2_temp view.")
    logger.info("Creating direccion1 view.")
    logger.info("Creating direccion2 view.")
    logger.info("Creating GPO_AT2_Direcciones_Campos view.")

    # create auxiliary tables for directions 1 and 2
    for direction in [1, 2]:
        # execute query for direction
        loc_sdf = spark.sql(Direcciones_Campos_Aux_Query.format(direction))

        # register temporary view
        loc_sdf.createOrReplaceTempView("direccion{}_temp".format(direction))

    # qualify tables for directions 1 and 2
    for direction_Qualify in [1, 2]:
        # execute query for direction
        loc_sdf = spark.sql(Direcciones_Campos_qualify_query.format(direction_Qualify))

        # register temporary view
        loc_sdf.createOrReplaceTempView("direccion{}".format(direction_Qualify))

    # create the final table using directions 1 and 2
    # execute query and register view
    Direcciones_Campos_sdf    = spark.sql(Direcciones_Campos_query)
    Direcciones_Campos_sdf.createOrReplaceTempView(
        'GPO_AT2_Direcciones_Campos'
    )

    #####################################################################################
    logger.info("Creating GPO_AT2_universo_inicial_ref view.")
    # execute query
    establ_Prin_Univ_aux_1_sdf = spark.sql(establ_Prin_Univ_aux_1_Query)

    # select columns
    establ_Prin_Univ_aux_1_sdf = establ_Prin_Univ_aux_1_sdf.select(
        "nro_establecimiento" ,
        "cuit_establecimiento_host",
        "nro_terminal",
        "cod_rubro",
        "desc_rubro",
        "nombre_fantasia",
        "beneficiario_host",
        "cod_estado_establecimiento",
        "desc_estado_establecimiento",
        "nivel_seguridad_moto_ecom",
        "bandera",
        "latitud",
        "longitud",
        "desc_departamento_geo",
        "desc_provincia_geo",
        "desc_localidad_gba_geo",
        "cod_provincia",
        "desc_provincia",
        "nombre_calle",
        "numero_calle",
        "codigo_postal",
        "cod_localidad",
        "localidad", 
        "direccion"
    )

    # register temporary view
    establ_Prin_Univ_aux_1_sdf.createOrReplaceTempView(
        "GPO_AT2_universo_inicial_ref"
    )

    ######################################################################################    
    logger.info("Creating GPO_AT2_universo_inicial_ref2_aux view.")

    # execute query
    establ_Prin_Univ_aux_2_sdf = spark.sql(establ_Prin_Univ_aux_2_Query)

    # select columns
    establ_Prin_Univ_aux_2_sdf = establ_Prin_Univ_aux_2_sdf.select(
        "nro_establecimiento",
        "nro_terminal"
    )

    # register temporary view
    establ_Prin_Univ_aux_2_sdf.createOrReplaceTempView(
        "GPO_AT2_universo_inicial_ref2_aux"
    )

    ######################################################################################   
    logger.info("Creating GPO_AT2_universo_inicial_ref2 view.")

    # execute query
    establ_Prin_Univ_aux_3_sdf = spark.sql(establ_Prin_Univ_aux_3_Query.format(firstday, lastday))

    # select columns
    establ_Prin_Univ_aux_3_sdf = establ_Prin_Univ_aux_3_sdf.select(
        "nro_establecimiento",
        "cuit_establecimiento_host",
        "nro_terminal"
    )

    # register temporary view
    establ_Prin_Univ_aux_3_sdf.createOrReplaceTempView(
        "GPO_AT2_universo_inicial_ref2"
    )

    ######################################################################################
    logger.info("Creating GPO_AT2_universo_inicial_ref3 view.")

    # execute query
    establ_Prin_Univ_sdf = spark.sql(establ_Prin_Univ_Query)

    # select columns
    establ_Prin_Univ_sdf = establ_Prin_Univ_sdf.select(
        "nro_establecimiento",
        "nro_establecimiento_ref",
        "cuit_establecimiento_host_ref",
        "nro_terminal",
        "cod_rubro",
        "desc_rubro",
        "nombre_fantasia",
        "beneficiario_host",
        "cod_estado_establecimiento",
        "desc_estado_establecimiento",
        "nivel_seguridad_moto_ecom",
        "bandera",
        "latitud",
        "longitud",
        "desc_departamento_geo",
        "desc_provincia_geo",
        "desc_localidad_gba_geo",
        "cod_provincia",
        "desc_provincia",
        "nombre_calle",
        "numero_calle",
        "codigo_postal",
        "cod_localidad",
        "localidad", 
        "direccion"
    )

    # dropna
    establ_Prin_Univ_sdf = establ_Prin_Univ_sdf.dropna(subset=["nro_establecimiento_ref"])

    # register temporary view
    establ_Prin_Univ_sdf.createOrReplaceTempView(
        "GPO_AT2_universo_inicial_ref3"
    )


    ######################################################################################
    # ----- create aux table including distinct nro_establ
    logger.info("Creating GPO_AT2_universo_inicial_ref3_distinct view.")
    ui_distinct_ref3_sdf = establ_Prin_Univ_sdf.select(
        "nro_establecimiento",
        "nro_establecimiento_ref",
        "cuit_establecimiento_host_ref"
    ).distinct()

    # drop duplicates
    ui_distinct_ref3_sdf = ui_distinct_ref3_sdf.dropDuplicates()

    # register temporary view
    ui_distinct_ref3_sdf.createOrReplaceTempView("GPO_AT2_universo_inicial_ref3_distinct")

    ######################################################################################
    # ----- create aux table counting establishments for each month
    logger.info("Create aux tables counting establishments for each month.")
    #date_ref_aux = date_ref + timedelta(1)
    delta_period = 0
    date_ref_aux = date_ref + relativedelta(months=delta_period) + timedelta(days=-delta_period)

    for month_ref in range(1, 13):
        logger.info('Creating aggregated table for month {}'.format(month_ref))
        # update month string to pass as variable name
        month_name = month_ref #-1 if month_ref != 0 else ''

        # define table name
        month_autoritation_table_name = 'month{}_establecimiento_Principal_cant'.format(month_name)

        # get month references
        month_end = date_ref_aux.replace(day=1) - timedelta(days=1)
        month_start = date_ref_aux.replace(day=1) - timedelta(days=month_end.day)
        date_ref_aux = month_end  # update date_ref_aux


        month_sdf = spark.sql(month_aut_fmt_query.format(
            month_name,
            month_name,
            month_start,
            month_end
        ))

        # create the temporary view
        month_sdf.createOrReplaceTempView(month_autoritation_table_name)
    
    ######################################################################################
    logger.info("Creating cant_term_ref view.")

    # execute query
    cant_term_sdf = spark.sql(cant_term_ref_query)

    # register temporary view
    cant_term_sdf.createOrReplaceTempView(
        "cant_term_ref"
    )
        
    ######################################################################################
    logger.info("Creating GPO_AT2_txs_estab_ref view.")
    # execute query
    establ_Prin_Transacciones_Aux_sdf = spark.sql(establ_Prin_Transacciones_Aux_query)

    # dropna from establ_Prin_Transacciones_Aux_sdf and fillna
    establ_Prin_Transacciones_Aux_sdf = establ_Prin_Transacciones_Aux_sdf.dropna(subset=["nro_establecimiento_ref"])
    establ_Prin_Transacciones_Aux_sdf = establ_Prin_Transacciones_Aux_sdf.fillna(0.0)

    # register temporary view
    establ_Prin_Transacciones_Aux_sdf.createOrReplaceTempView(
        "GPO_AT2_txs_estab_ref"
    )

    ######################################################################################
    logger.info("Creating GPO_AT2_txs_Establecimiento_Ref view.")

    # groupby data to produce GPO_AT2_txs_Establecimiento_Ref
    establ_Prin_Transacciones_sdf = spark.sql(establ_Prin_Transacciones_query)

    # register temporary view
    establ_Prin_Transacciones_sdf.createOrReplaceTempView(
        "GPO_AT2_txs_Establecimiento_Ref"
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
    logger.info("Creating GPO_ACT_Parque_Instalado_Aux_2_1 view.")

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
    logger.info("Creating GPO_AT2_niveles_activacion_Establecimiento_ref_aux view.")
    # execute query
    establ_Prin_Niveles_Activacion_Aux_1_sdf = spark.sql(establ_Prin_Niveles_Activacion_Aux_1_query.format(firstday, lastday,firstday, lastday))

    # register temporary view
    establ_Prin_Niveles_Activacion_Aux_1_sdf.createOrReplaceTempView(
        "GPO_AT2_niveles_activacion_Establecimiento_ref_aux"
    )

    ######################################################################################
    logger.info("Creating GPO_AT2_niveles_activacion_ref view.")
    # execute query
    establ_Prin_Niveles_Activacion_Aux_2_sdf = spark.sql(establ_Prin_Niveles_Activacion_Aux_2_query)

    # register temporary view
    establ_Prin_Niveles_Activacion_Aux_2_sdf.createOrReplaceTempView(
        "GPO_AT2_niveles_activacion_ref"
    )

    ######################################################################################
    logger.info("Creating GPO_AT2_niveles_activacion_Establecimiento_Ref view.")
    # execute query
    establ_Prin_Niveles_Activacion_sdf = spark.sql(establ_Prin_Niveles_Activacion_query)

    # register temporary view
    establ_Prin_Niveles_Activacion_sdf.createOrReplaceTempView(
        "GPO_AT2_niveles_activacion_Establecimiento_Ref"
    )

    ######################################################################################
    logger.info("Creating GPO_AT2_dias_terminales_Establecimiento_ref_aux view.")
    # execute query
    establ_Prin_Dias_Terminales_Aux_1_sdf = spark.sql(establ_Prin_Dias_Terminales_Aux_1_query.format(firstday, lastday,firstday, lastday))

    # register temporary view
    establ_Prin_Dias_Terminales_Aux_1_sdf.createOrReplaceTempView(
        "GPO_AT2_dias_terminales_Establecimiento_ref_aux"
    )

    ######################################################################################
    logger.info("Creating GPO_AT2_dias_terminales_ref view.")
    # execute query
    establ_Prin_Dias_Terminales_Aux_2_sdf = spark.sql(establ_Prin_Dias_Terminales_Aux_2_query)

    # dropna
    establ_Prin_Dias_Terminales_Aux_2_sdf = establ_Prin_Dias_Terminales_Aux_2_sdf.dropna(subset=["nro_establecimiento_ref"])

    # register temporary view
    establ_Prin_Dias_Terminales_Aux_2_sdf.createOrReplaceTempView(
        "GPO_AT2_dias_terminales_ref"
    )

    #####################################################################################
    logger.info("Creating GPO_AT2_dias_terminales_Establecimiento_Ref view.")
    # execute query
    establ_Prin_Dias_Terminales_sdf = spark.sql(establ_Prin_Dias_Terminales_query)

    # register temporary view
    establ_Prin_Dias_Terminales_sdf.createOrReplaceTempView(
        "GPO_AT2_dias_terminales_Establecimiento_Ref"
    )

    #####################################################################################
    logger.info("Creating ABT_ESTABLECIMIENTO_PRINCIPAL view.")
    # execute query
    ABT_Establecimiento_Principal_sdf = spark.sql(ABT_Establecimiento_Principal_query)
    ABT_Establecimiento_Principal_sdf = ABT_Establecimiento_Principal_sdf.withColumn("fe_proceso", current_date())
    ABT_Establecimiento_Principal_sdf = ABT_Establecimiento_Principal_sdf.withColumn("id_periodo_analisis", date_format(add_months(to_date(col("fe_proceso"),"yyyy-MM"),-1),"yyyy-MM"))  

    # register temporary view
    ABT_Establecimiento_Principal_sdf.createOrReplaceTempView(
        "ABT_Establecimiento_Principal_M"
    ) 

    # ----- transform spark dataframe to pandas and write to parquet
    logger.info("Writing ABT_Establecimiento_Principal_M to parquet.")
    # repartition dataframe
    ABT_Establecimiento_Principal_sdf = ABT_Establecimiento_Principal_sdf.repartition(total_cores * 3)

    # write to output
    output_path = "s3://" + os.path.join(
        output_bucket, output_prefix, "establecimiento_principal_dataset.snappy.parquet")
    ABT_Establecimiento_Principal_sdf.write.mode('overwrite').parquet(output_path, compression='snappy')
