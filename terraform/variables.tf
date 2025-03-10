variable "credentials" {
  description = "Credentials"
  default     = "~/.config/gcloud/application_default_credentials.json"
}

variable "project" {
  description = "Project"
  default     = "de-tasks-453115"
}

variable "region" {
  description = "Region"
  default     = "europe-central2"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "exchange_rates"
}
