variable "function_name" {
  description = "Lambda function name; also used to name its role, log group and failure queue."
  type        = string
}

variable "runtime" {
  description = "Lambda runtime."
  type        = string
  default     = "python3.12"
}

variable "source_files" {
  description = "Files packaged into the Lambda zip: map of file name inside the zip to local source path."
  type        = map(string)
}

variable "table_name" {
  description = "Current-state DynamoDB table name, passed to the function as TABLE_NAME."
  type        = string
}

variable "table_arn" {
  description = "ARN of the current-state table (scopes dynamodb:PutItem)."
  type        = string
}

variable "stream_arn" {
  description = "ARN of the Kinesis stream the function consumes."
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
}

variable "batch_size" {
  description = "Maximum records per Lambda invocation."
  type        = number
  default     = 100
}

variable "max_batching_window_seconds" {
  description = "Seconds Lambda waits to fill a batch."
  type        = number
  default     = 5
}

variable "max_retry_attempts" {
  description = "Retries of a failing batch before it goes to the failure queue."
  type        = number
  default     = 3
}

variable "max_record_age_seconds" {
  description = "Records older than this are skipped and sent to the failure queue."
  type        = number
  default     = 3600
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
