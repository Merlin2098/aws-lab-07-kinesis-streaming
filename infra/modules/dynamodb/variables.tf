variable "table_name" {
  description = "DynamoDB table holding the current state of each package."
  type        = string
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
