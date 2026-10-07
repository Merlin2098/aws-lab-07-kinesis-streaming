output "table_name" {
  description = "Name of the current-state table."
  value       = aws_dynamodb_table.current_state.name
}

output "resource_arn" {
  description = "ARN of the current-state table."
  value       = aws_dynamodb_table.current_state.arn
}

output "log_group_name" {
  description = "Empty by design: DynamoDB does not emit logs."
  value       = ""
}

output "log_group_arn" {
  description = "Empty by design: DynamoDB does not emit logs."
  value       = ""
}
