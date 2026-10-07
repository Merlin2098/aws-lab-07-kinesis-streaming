output "budget_name" {
  description = "AWS Budget name for monthly cost governance. Empty string when enable_budget_guardrail is false."
  value       = var.enable_budget_guardrail ? aws_budgets_budget.monthly[0].name : ""
}

output "budget_alert_sns_arn" {
  description = "SNS topic ARN for budget alerts. Empty string when the guardrail is disabled or no alert email is configured."
  value       = var.enable_budget_guardrail && var.budget_alert_email != "" ? aws_sns_topic.budget_alerts[0].arn : ""
}

output "stream_name" {
  description = "Kinesis data stream name (STREAM_NAME for the producer)."
  value       = module.kinesis.stream_name
}

output "table_name" {
  description = "DynamoDB current-state table name."
  value       = module.dynamodb.table_name
}

output "lambda_function_name" {
  description = "Consumer Lambda function name."
  value       = module.consumer.function_name
}

output "failure_queue_url" {
  description = "SQS queue receiving failed batches (metadata only)."
  value       = module.consumer.failure_queue_url
}

output "datalake_bucket_name" {
  description = "S3 data lake bucket name."
  value       = module.datalake.bucket_name
}

output "firehose_delivery_stream_name" {
  description = "Firehose delivery stream name."
  value       = module.datalake.delivery_stream_name
}

output "consumer_log_group_name" {
  description = "CloudWatch log group of the consumer Lambda."
  value       = module.consumer.log_group_name
}

output "firehose_log_group_name" {
  description = "CloudWatch log group of the Firehose delivery stream."
  value       = module.datalake.log_group_name
}
