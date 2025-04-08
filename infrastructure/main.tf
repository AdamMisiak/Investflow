provider "google" {
  project = var.project_id
  region  = var.region
}

# check name of the bucket and tf resource
resource "google_storage_bucket" "csv_reports_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket_object" "function_source" {
  name   = "function-source.zip"
  bucket = google_storage_bucket.csv_reports_bucket.name
  source = "function-source.zip"
}

resource "google_cloudfunctions_function" "csv_reports_processor" {
  name        = "csv-reports-processor"
  description = "Parses CSV files uploaded to GCS bucket"
  runtime     = "python310"
  region      = var.region

  available_memory_mb   = 128
  source_archive_bucket = google_storage_bucket.csv_reports_bucket.name
  source_archive_object = google_storage_bucket_object.function_source.name
  entry_point           = "process_csv"

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.csv_reports_bucket.name
  }

  environment_variables = {
    BUCKET_NAME = google_storage_bucket.csv_reports_bucket.name
  }
}
