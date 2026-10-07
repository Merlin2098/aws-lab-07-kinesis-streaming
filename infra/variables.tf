variable "project_name" {
  description = "Project name used in AWS resource naming."
  type        = string
  default     = "logistics-streaming"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}

variable "owner" {
  description = "Owner tag applied to all managed resources."
  type        = string
  default     = "data-engineering"
}

variable "aws_region" {
  description = "AWS region for the deployment."
  type        = string
  default     = "us-east-1"
}

variable "tags" {
  description = "Additional tags applied to all resources."
  type        = map(string)
  default     = {}
}

variable "cost_center" {
  description = "Cost center tag for budget allocation and cost reporting."
  type        = string
  default     = "engineering"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days. Use 7 for demos and labs; set higher for production per compliance requirements."
  type        = number
  default     = 7
}

variable "enable_budget_guardrail" {
  description = "Whether to create the monthly AWS Budget (and its SNS alert topic, if an email is set). Off by default for student/demo use; enable for real deployments."
  type        = bool
  default     = false
}

variable "budget_limit_usd" {
  description = "Monthly AWS budget limit in USD. Alerts fire at 80% (actual) and 100% (forecasted). Only used when enable_budget_guardrail is true."
  type        = number
  default     = 25
}

variable "budget_alert_email" {
  description = "Email address for budget alerts. Leave empty to skip SNS subscription creation. Only used when enable_budget_guardrail is true."
  type        = string
  default     = ""
}

variable "stream_name" {
  description = "Kinesis data stream name."
  type        = string
  default     = "logistics-events"
}

variable "shard_count" {
  description = "Kinesis shards (PROVISIONED). Two make the partition key visible in the demo."
  type        = number
  default     = 2
}

variable "stream_retention_hours" {
  description = "Hours Kinesis retains records."
  type        = number
  default     = 24
}

variable "datalake_partition_granularity" {
  description = "S3 time partitioning: \"hour\" (realistic) or \"minute\" (live demo only)."
  type        = string
  default     = "hour"
}

variable "firehose_buffer_interval_seconds" {
  description = "Seconds Firehose buffers before writing to S3. 60 keeps the demo short."
  type        = number
  default     = 60
}

variable "datalake_force_destroy" {
  description = "Whether terraform destroy may delete the data lake bucket while it holds objects. Keep true for the lab."
  type        = bool
  default     = true
}
