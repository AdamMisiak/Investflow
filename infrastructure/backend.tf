terraform {
  backend "gcs" {
    bucket  = "investflow-terraform-state-bucket"
  }
}
