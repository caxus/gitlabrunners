data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  tags = {
    Project = var.project_name
  }
  invoke_pipelines_role = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.invoke_pipelines_role}"
  pipeline_role_arn     = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.pipeline_role_arn}"
}

###############################################################################
########################## SAGEMAKER PIPELINES ################################
###############################################################################
resource "aws_cloudformation_stack" "sagemaker_pipelines" {
  name = "sagemaker-pipelines"
  parameters = {
    pProjectName           = var.project_name
    pPipelineRoleArn       = local.pipeline_role_arn
    pFGBucketPrepareOutput = var.fg_bucket_prepare_output
    pFGPrefixOutput        = var.fg_prefix_output
    pBucketNameCodigos     = var.bucket_name_codigos
    pFeatureStoreBucket    = var.feature_store_bucket
    pFeatureStorePrefix    = var.feature_store_prefix
    pSQLQueryPath          = var.sql_query_path
  }
  template_body = file("${path.module}/sagemaker_pipelines.yml")
}

output "test_CF_output" {
  value = aws_cloudformation_stack.sagemaker_pipelines.outputs
}

###############################################################################
########################## EVENTBRIDGE ########################################
###############################################################################

resource "aws_cloudwatch_event_rule" "rule_1" {
  name                = "${var.project_name}-invoke-featuregroup-pipelines1"
  description         = "Scheduled Rule to invoke SageMaker Pipelines that ingest data monthly to a set Feature Groups"
  schedule_expression = "cron(0 16 * * ? *)"
  is_enabled          = false
}

resource "aws_cloudwatch_event_target" "target_fg_establecimiento" {
  rule      = aws_cloudwatch_event_rule.rule_1.name
  target_id = "target_fg_establecimiento"
  arn       = "arn:aws:sagemaker:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:pipeline/fgestablecimiento"
  role_arn  = local.invoke_pipelines_role
}

resource "aws_cloudwatch_event_target" "target_fg_establecimiento_principal" {
  rule      = aws_cloudwatch_event_rule.rule_1.name
  target_id = "target_fg_establecimiento_principal"
  arn       = "arn:aws:sagemaker:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:pipeline/fgestablecimientoprincipal"
  role_arn  = local.invoke_pipelines_role
}

resource "aws_cloudwatch_event_target" "target_fg_target_at2" {
  rule      = aws_cloudwatch_event_rule.rule_1.name
  target_id = "target_fg_target_at2"
  arn       = "arn:aws:sagemaker:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:pipeline/fgtargetat2"
  role_arn  = local.invoke_pipelines_role
}

resource "aws_cloudwatch_event_rule" "rule_2" {
  name                = "${var.project_name}-invoke-featuregroup-pipelines2"
  description         = "Scheduled Rule to invoke SageMaker Pipelines that ingest data monthly to a set Feature Groups"
  schedule_expression = "cron(25 16 * * ? *)"
  is_enabled          = false
}

resource "aws_cloudwatch_event_target" "target_fg_model_at2" {
  rule      = aws_cloudwatch_event_rule.rule_2.name
  target_id = "target_fg_model_at2"
  arn       = "arn:aws:sagemaker:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:pipeline/fgmodelat2"
  role_arn  = local.invoke_pipelines_role
}

###############################################################################
########################## SSM PARAMETERS #####################################
###############################################################################
resource "aws_ssm_parameter" "ssm_parameter_authorization" {
  name        = "S3PATH_AUTORIZACION_TABLE"
  description = "S3 Path para la tabla 'autorizacion' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_authorization
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_banco" {
  name        = "S3PATH_BANCO_TABLE"
  description = "S3 Path para la tabla 'banco' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_banco
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_bt_adq_establ" {
  name        = "S3PATH_BT_ADQ_AUT_ESTABL_V_TABLE"
  description = "S3 Path para la tabla 'bt_adq_autorizacion_establecimiento_v' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_bt_adq_establ
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_ciclo_pago_establ" {
  name        = "S3PATH_CICLO_PAGO_ESTABLECIMIENTO_TABLE"
  description = "S3 Path para la tabla de 'ciclo_pago_establecimiento' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_ciclo_pago_establ
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_dir_establ_geo_ult" {
  name        = "S3PATH_DIR_ESTABLECIMIENTO_GEO_ULT_INST_TABLE"
  description = "S3 Path para la tabla de 'direccion_establecimiento_geolocalizado_ult_inst' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_dir_establ_geo_ult
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_dir_establ_hist" {
  name        = "S3PATH_DIR_ESTABLECIMIENTO_HIST_TABLE"
  description = "S3 Path para la tabla de 'direccion_establecimiento_hist' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_dir_establ_hist
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_establ_hist" {
  name        = "S3PATH_ESTABLECIMIENTO_HIST_TABLE"
  description = "S3 Path para la tabla de 'establecimiento' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_establ_hist
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_estado_establ" {
  name        = "S3PATH_ESTADO_ESTABLECIMIENTO_TABLE"
  description = "S3 Path para la tabla de 'estado_establecimiento' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_estado_establ
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_habilitacion_cuotas" {
  name        = "S3PATH_HABILITACION_CUOTAS_TABLE"
  description = "S3 Path para la tabla de 'habilitacion_cuotas' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_habilitacion_cuotas
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_inflacion" {
  name        = "S3PATH_INFLACION_TABLE"
  description = "S3 Path para la tabla de inflacion consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_inflacion
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_lapos_contrato_establ" {
  name        = "S3PATH_LAPOS_CONTRATOESTABL_TABLE"
  description = "S3 Path para la tabla 'lapos_contrato_establecimiento' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_lapos_contrato_establ
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_lapos_fact_hist" {
  name        = "S3PATH_LAPOS_FACTHIST_TABLE"
  description = "S3 Path para la tabla 'lapos_facthist' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_lapos_fact_hist
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_lapos_log_user" {
  name        = "S3PATH_LAPOS_LOGUSER_TABLE"
  description = "S3 Path para la tabla 'lapos_loguser' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_lapos_log_user
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_lapos_plantilla_adec" {
  name        = "S3PATH_LAPOS_PLANTILLA_ADECUADA_TABLE"
  description = "S3 Path para la tabla 'lapos_plantilla_adecuada' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_lapos_plantilla_adec
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_lapos_terminales_vip" {
  name        = "S3PATH_LAPOS_TERMINALESVIP_TABLE"
  description = "S3 Path para la tabla 'lapos_terminalesvip' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_lapos_terminales_vip
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_lapos_terminal_modelo" {
  name        = "S3PATH_LAPOS_TERMINAL_MODELO_TABLE"
  description = "S3 Path para la tabla 'lapos_terminal_modelo' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_lapos_terminal_modelo
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_lapos_terminal" {
  name        = "S3PATH_LAPOS_TERMINAL_TABLE"
  description = "S3 Path para la tabla 'lapos_terminal' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_lapos_terminal
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_mov_presentado" {
  name        = "S3PATH_MOVIMIENTO_PRESENTADO_TABLE"
  description = "S3 Path para la tabla de 'movimiento_presentado' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_mov_presentado
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_provincia" {
  name        = "S3PATH_PROVINCIA_TABLE"
  description = "S3 Path para la tabla de 'provincia' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_provincia
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_rubro" {
  name        = "S3PATH_RUBRO_TABLE"
  description = "S3 Path para la tabla de 'rubro' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_rubro
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_telef_establ" {
  name        = "S3PATH_TELEFONO_ESTABL_TABLE"
  description = "S3 Path para la tabla 'telefono_establecimiento' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_telef_establ
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_terminal" {
  name        = "S3PATH_TERMINAL_HIST_TABLE"
  description = "S3 Path para la tabla 'terminal' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_terminal
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_tipo_contrib_ing" {
  name        = "S3PATH_TIPO_CONTRIB_ING_BRUTOS_TABLE"
  description = "S3 Path para la tabla de 'tipo_contrib_ing_brutos' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_tipo_contrib_ing
  tier        = "Standard"
  tags        = local.tags
}

resource "aws_ssm_parameter" "ssm_parameter_tipo_iva" {
  name        = "S3PATH_TIPO_IVA_TABLE"
  description = "S3 Path para la tabla de 'tipo_iva' consumida por los pipelines de Feature Groups"
  type        = "String"
  value       = var.s3_path_tipo_iva
  tier        = "Standard"
  tags        = local.tags
}