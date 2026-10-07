output "function_name" {
  description = "Name of the consumer Lambda function."
  value       = aws_lambda_function.this.function_name
}

output "resource_arn" {
  description = "ARN of the consumer Lambda function."
  value       = aws_lambda_function.this.arn
}

output "log_group_name" {
  description = "CloudWatch log group of the consumer."
  value       = aws_cloudwatch_log_group.this.name
}

output "log_group_arn" {
  description = "ARN of the consumer log group."
  value       = aws_cloudwatch_log_group.this.arn
}

output "failure_queue_url" {
  description = "URL of the SQS queue that receives failed batches."
  value       = aws_sqs_queue.failures.url
}

output "failure_queue_arn" {
  description = "ARN of the SQS queue that receives failed batches."
  value       = aws_sqs_queue.failures.arn
}
