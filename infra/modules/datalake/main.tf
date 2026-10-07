data "aws_caller_identity" "current" {}

locals {
  bucket_name   = "${var.name_prefix}-${data.aws_caller_identity.current.account_id}-datalake"
  firehose_name = "${var.name_prefix}-events-to-s3"

  # The !{timestamp:...} namespace uses the time the record ARRIVES at Firehose,
  # not the event's own timestamp.
  date_prefix   = "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
  minute_prefix = var.partition_granularity == "minute" ? "minute=!{timestamp:mm}/" : ""
  events_prefix = "events/${local.date_prefix}${local.minute_prefix}"
  errors_prefix = "errors/!{firehose:error-output-type}/${local.date_prefix}"
}

resource "aws_s3_bucket" "datalake" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy
  tags          = var.tags
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.firehose_name}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_stream" "delivery" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${local.firehose_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "permissions" {
  statement {
    sid = "WriteToBucket"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      aws_s3_bucket.datalake.arn,
      "${aws_s3_bucket.datalake.arn}/*",
    ]
  }

  statement {
    sid = "ReadSourceStream"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
    ]
    resources = [var.stream_arn]
  }

  statement {
    sid       = "WriteDeliveryLogs"
    actions   = ["logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.firehose.arn}:*"]
  }

  statement {
    sid       = "DenyUnusedActions"
    effect    = "Deny"
    actions   = ["dynamodb:*", "lambda:*", "iam:*", "sqs:*"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${local.firehose_name}-permissions"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.permissions.json
}

resource "aws_kinesis_firehose_delivery_stream" "events" {
  name        = local.firehose_name
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = var.stream_arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.datalake.arn
    prefix              = local.events_prefix
    error_output_prefix = local.errors_prefix
    buffering_size      = 1 # MB, the minimum; the interval is what the demo waits on
    buffering_interval  = var.buffer_interval_seconds
    compression_format  = "UNCOMPRESSED"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.delivery.name
    }
  }

  depends_on = [aws_iam_role_policy.firehose]
  tags       = var.tags
}
