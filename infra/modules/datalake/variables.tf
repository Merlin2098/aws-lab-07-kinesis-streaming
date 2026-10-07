variable "name_prefix" {
  description = "Prefix for resource names, e.g. logistics-streaming-dev."
  type        = string
}

variable "stream_arn" {
  description = "ARN of the Kinesis stream Firehose reads from."
  type        = string
}

variable "partition_granularity" {
  description = "Time partitioning of S3 keys: hour (realistic) or minute (live demo only; produces many small objects)."
  type        = string

  validation {
    condition     = contains(["hour", "minute"], var.partition_granularity)
    error_message = "partition_granularity must be \"hour\" or \"minute\"."
  }
}

variable "buffer_interval_seconds" {
  description = "Seconds Firehose buffers before writing an object to S3 (0 to 900)."
  type        = number

  validation {
    condition     = var.buffer_interval_seconds >= 0 && var.buffer_interval_seconds <= 900
    error_message = "buffer_interval_seconds must be between 0 and 900."
  }
}

variable "force_destroy" {
  description = "Whether terraform destroy may delete the bucket while it still holds objects."
  type        = bool
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
