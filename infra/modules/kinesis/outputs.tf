output "stream_name" {
  description = "Name of the Kinesis data stream."
  value       = aws_kinesis_stream.this.name
}

output "resource_arn" {
  description = "ARN of the Kinesis data stream."
  value       = aws_kinesis_stream.this.arn
}

output "log_group_name" {
  description = "Empty by design: Kinesis Data Streams does not emit logs."
  value       = ""
}

output "log_group_arn" {
  description = "Empty by design: Kinesis Data Streams does not emit logs."
  value       = ""
}
