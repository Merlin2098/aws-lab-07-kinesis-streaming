variable "stream_name" {
  description = "Kinesis data stream name."
  type        = string
}

variable "shard_count" {
  description = "Number of shards (PROVISIONED mode). Two shards make the partition key visible in the demo."
  type        = number

  validation {
    condition     = var.shard_count >= 1
    error_message = "shard_count must be at least 1."
  }
}

variable "retention_hours" {
  description = "Hours Kinesis retains records (24 to 8760)."
  type        = number

  validation {
    condition     = var.retention_hours >= 24 && var.retention_hours <= 8760
    error_message = "retention_hours must be between 24 and 8760."
  }
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
