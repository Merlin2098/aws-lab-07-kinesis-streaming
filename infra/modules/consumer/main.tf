data "archive_file" "package" {
  type        = "zip"
  output_path = "${path.root}/.build/${var.function_name}.zip"

  dynamic "source" {
    for_each = var.source_files
    content {
      filename = source.key
      content  = file(source.value)
    }
  }
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# Kinesis failure destinations carry only metadata (shard and sequence-number
# range), not the record bodies; the events stay readable in the stream until
# its retention expires.
resource "aws_sqs_queue" "failures" {
  name                      = "${var.function_name}-failures"
  sqs_managed_sse_enabled   = true
  message_retention_seconds = 1209600
  tags                      = var.tags
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name               = "${var.function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "permissions" {
  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.this.arn}:*"]
  }

  statement {
    sid = "ReadStream"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
    ]
    resources = [var.stream_arn]
  }

  statement {
    sid       = "WriteCurrentState"
    actions   = ["dynamodb:PutItem"]
    resources = [var.table_arn]
  }

  # The failure queue lives in this module, so its permission is declared here
  # (AGENTS.md: IAM cross-module placement rule).
  statement {
    sid       = "SendToFailureQueue"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.failures.arn]
  }

  statement {
    sid    = "DenyUnusedActions"
    effect = "Deny"
    actions = [
      "dynamodb:DeleteItem",
      "dynamodb:DeleteTable",
      "dynamodb:UpdateTable",
      "iam:*",
      "s3:*",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "this" {
  name   = "${var.function_name}-permissions"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.permissions.json
}

resource "aws_lambda_function" "this" {
  function_name    = var.function_name
  role             = aws_iam_role.this.arn
  runtime          = var.runtime
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.package.output_path
  source_code_hash = data.archive_file.package.output_base64sha256
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      TABLE_NAME = var.table_name
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.this.name
  }

  depends_on = [aws_cloudwatch_log_group.this, aws_iam_role_policy.this]
  tags       = var.tags
}

resource "aws_lambda_event_source_mapping" "stream" {
  event_source_arn                   = var.stream_arn
  function_name                      = aws_lambda_function.this.arn
  starting_position                  = "LATEST"
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.max_batching_window_seconds
  maximum_retry_attempts             = var.max_retry_attempts
  maximum_record_age_in_seconds      = var.max_record_age_seconds
  bisect_batch_on_function_error     = true
  function_response_types            = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.failures.arn
    }
  }

  tags       = var.tags
  depends_on = [aws_iam_role_policy.this]
}
