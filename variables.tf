variable "project_name" {
  type        = string
  description = "Name of the project."
}

variable "fg_bucket_prepare_output" {
  type        = string
  description = "Bucket name where data prepared with PySpark step will be stored."
}

variable "fg_prefix_output" {
  type        = string
  description = "Prefix name where data prepared with PySpark step will be stored."
}

variable "feature_store_bucket" {
  type        = string
  description = "Bucket name used for the feature store."
}

variable "feature_store_prefix" {
  type        = string
  description = "Prefix name of where Feature Groups will be stored in the Feature Store. Associated to pFeatureStoreBucket."
}

variable "bucket_name_codigos" {
  type        = string
  description = "Name of bucket which store pipeline codes."
}

variable "sql_query_path" {
  type        = string
  description = "S3 path where SQL queries will be written."
}

variable "pipeline_role_arn" {
  type        = string
  description = "Name of the Role used to execute the sagemaker pipeline."
}

variable "s3_path_authorization" {
  type        = string
  description = "S3 path where table AUTORIZACION is stored."
}

variable "s3_path_banco" {
  type        = string
  description = "S3 path where table BANCO is stored."
}

variable "s3_path_bt_adq_establ" {
  type        = string
  description = "S3 path where table BT_ADQ_AUTORIZACION_ESTABLECIMIENTO_V is stored."
}

variable "s3_path_ciclo_pago_establ" {
  type        = string
  description = "S3 path where table CICLO_PAGO_ESTABLECIMIENTO is stored."
}

variable "s3_path_dir_establ_geo_ult" {
  type        = string
  description = "S3 path where table DIR_ESTABLECIMIENTO_GEO_ULT_INST is stored."
}

variable "s3_path_dir_establ_hist" {
  type        = string
  description = "S3 path where table DIR_ESTABLECIMIENTO_HIST is stored."
}

variable "s3_path_establ_hist" {
  type        = string
  description = "S3 path where table ESTABLECIMIENTO_HIST is stored."
}

variable "s3_path_estado_establ" {
  type        = string
  description = "S3 path where table ESTADO_ESTABLECIMIENTO is stored."
}

variable "s3_path_habilitacion_cuotas" {
  type        = string
  description = "S3 path where table HABILITACION_CUOTAS is stored."
}

variable "s3_path_inflacion" {
  type        = string
  description = "S3 path where table INFLACION is stored."
}

variable "s3_path_lapos_contrato_establ" {
  type        = string
  description = "S3 path where table LAPOS_CONTRATO_ESTABLECIMIENTO is stored."
}

variable "s3_path_lapos_fact_hist" {
  type        = string
  description = "S3 path where table LAPOS_FACTHIST is stored."
}

variable "s3_path_lapos_log_user" {
  type        = string
  description = "S3 path where table LAPOS_LOGUSER is stored."
}

variable "s3_path_lapos_plantilla_adec" {
  type        = string
  description = "S3 path where table LAPOS_PLANTILLA_ADECUADA is stored."
}

variable "s3_path_lapos_terminales_vip" {
  type        = string
  description = "S3 path where table LAPOS_TERMINALESVIP is stored."
}

variable "s3_path_lapos_terminal_modelo" {
  type        = string
  description = "S3 path where table LAPOS_TERMINAL_MODELO is stored."
}

variable "s3_path_lapos_terminal" {
  type        = string
  description = "S3 path where table LAPOS_TERMINAL is stored."
}

variable "s3_path_mov_presentado" {
  type        = string
  description = "S3 path where table MOVIMIENTO_PRESENTADO is stored."
}

variable "s3_path_provincia" {
  type        = string
  description = "S3 path where table PROVINCIA is stored."
}

variable "s3_path_rubro" {
  type        = string
  description = "S3 path where table RUBRO is stored."
}

variable "s3_path_telef_establ" {
  type        = string
  description = "S3 path where table TELEFONO_ESTABLECIMIENTO is stored."
}

variable "s3_path_terminal" {
  type        = string
  description = "S3 path where table TERMINAL is stored."
}

variable "s3_path_tipo_contrib_g" {
  type        = string
  description = "S3 path where table TIPO_CONTRIB_GANANCIAS is stored."
}

variable "s3_path_tipo_contrib_ing" {
  type        = string
  description = "S3 path where table TIPO_CONTRIB_ING_BRUTOS is stored."
}

variable "s3_path_tipo_iva" {
  type        = string
  description = "S3 path where table TIPO_IVA is stored."
}

variable "invoke_pipelines_role" {
  type        = string
  description = "Name of the role needed to invoke the pipeline."
}