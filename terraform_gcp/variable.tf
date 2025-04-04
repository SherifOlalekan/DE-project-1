variable "credentials" {
  description = "My Credentials"
  default     = "/home/olalekan/DE-project-1/keys/credk.json"
}


variable "project" {
  description = "Project"
  default     = "my-de-journey"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "africa-south1-a"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "africa-south1"
}

variable "bq_dataset_name" {
  description = "My Global Fashion Dataset project"
  #Update the below to what you want your dataset to be called
  default = "Fashion_retail_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "olalekan-de2753"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "REGIONAL"
}
