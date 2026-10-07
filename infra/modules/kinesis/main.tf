resource "aws_kinesis_stream" "this" {
  name             = var.stream_name
  shard_count      = var.shard_count
  retention_period = var.retention_hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  # debt: no server-side encryption, to keep the demo free of KMS key policies.
  # Upgrade trigger: before handling real data, set encryption_type = "KMS" and
  # grant kms:Decrypt to the consumer and Firehose roles.

  tags = var.tags
}
