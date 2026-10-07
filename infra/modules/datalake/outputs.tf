output "bucket_name" {
  description = "Name of the S3 data lake bucket."
  value       = aws_s3_bucket.datalake.bucket
}

output "bucket_arn" {
  description = "ARN of the S3 data lake bucket."
  value       = aws_s3_bucket.datalake.arn
}

output "delivery_stream_name" {
  description = "Name of the Firehose delivery stream."
  value       = aws_kinesis_firehose_delivery_stream.events.name
}

output "resource_arn" {
  description = "ARN of the Firehose delivery stream."
  value       = aws_kinesis_firehose_delivery_stream.events.arn
}

output "log_group_name" {
  description = "CloudWatch log group of the Firehose delivery stream."
  value       = aws_cloudwatch_log_group.firehose.name
}

output "log_group_arn" {
  description = "ARN of the Firehose log group."
  value       = aws_cloudwatch_log_group.firehose.arn
}
