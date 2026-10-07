resource "aws_dynamodb_table" "current_state" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "package_id"

  # Lab environment: the table must be removable with terraform destroy.
  deletion_protection_enabled = false

  attribute {
    name = "package_id"
    type = "S"
  }

  tags = var.tags
}
