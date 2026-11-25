# Remote state (documented, not required). CI runs `terraform init
# -backend=false`, so this file never needs credentials there.
#
# To use remote state, create the state bucket and lock table once (outside
# this configuration, so a destroy here cannot delete its own state), then
# uncomment the block and run `terraform init -migrate-state`:
#
# terraform {
#   backend "s3" {
#     bucket         = "<your-org>-tfstate"
#     key            = "ml-platform/dev/terraform.tfstate"
#     region         = "us-east-1"
#     encrypt        = true
#     kms_key_id     = "alias/<your-tfstate-key>"
#     dynamodb_table = "<your-tfstate-lock-table>"
#   }
# }
#
# State holds the generated Redis AUTH token, so the state bucket must be
# private, encrypted, versioned and readable only by the deploy role.
