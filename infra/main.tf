locals {
  name_prefix = lower(replace("${var.project_name}-${var.environment}", "_", "-"))
  common_tags = merge(
    var.tags,
    {
      Project     = var.project_name
      Environment = var.environment
      Owner       = var.owner
      ManagedBy   = "Terraform"
      CostCenter  = var.cost_center
    }
  )
}

module "kinesis" {
  source = "./modules/kinesis"

  stream_name     = var.stream_name
  shard_count     = var.shard_count
  retention_hours = var.stream_retention_hours
  tags            = local.common_tags
}

module "dynamodb" {
  source = "./modules/dynamodb"

  table_name = "${local.name_prefix}-package-state"
  tags       = local.common_tags
}

module "consumer" {
  source = "./modules/consumer"

  function_name = "${local.name_prefix}-consumer"
  source_files = {
    "handler.py"                  = "${path.module}/../src/lambda/consumer/handler.py"
    "contract.py"                 = "${path.module}/../src/lambda/consumer/contract.py"
    "package_event_contract.json" = "${path.module}/../src/contracts/package_event_contract.json"
  }
  table_name         = module.dynamodb.table_name
  table_arn          = module.dynamodb.resource_arn
  stream_arn         = module.kinesis.resource_arn
  log_retention_days = var.log_retention_days
  tags               = local.common_tags
}

module "datalake" {
  source = "./modules/datalake"

  name_prefix             = local.name_prefix
  stream_arn              = module.kinesis.resource_arn
  partition_granularity   = var.datalake_partition_granularity
  buffer_interval_seconds = var.firehose_buffer_interval_seconds
  force_destroy           = var.datalake_force_destroy
  log_retention_days      = var.log_retention_days
  tags                    = local.common_tags
}

resource "aws_sns_topic" "budget_alerts" {
  count = var.enable_budget_guardrail && var.budget_alert_email != "" ? 1 : 0
  name  = "${local.name_prefix}-budget-alerts"
  tags  = local.common_tags
}

resource "aws_sns_topic_subscription" "budget_email" {
  count     = var.enable_budget_guardrail && var.budget_alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.budget_alerts[0].arn
  protocol  = "email"
  endpoint  = var.budget_alert_email
}

resource "aws_budgets_budget" "monthly" {
  count             = var.enable_budget_guardrail ? 1 : 0
  name              = "${local.name_prefix}-monthly-budget"
  budget_type       = "COST"
  limit_amount      = tostring(var.budget_limit_usd)
  limit_unit        = "USD"
  time_unit         = "MONTHLY"
  time_period_start = "2024-01-01_00:00"
  tags              = local.common_tags

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:Project$${var.project_name}"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = var.budget_alert_email != "" ? [var.budget_alert_email] : []
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.budget_alert_email != "" ? [var.budget_alert_email] : []
  }
}
