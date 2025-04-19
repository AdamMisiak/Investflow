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
  detect_md5hash = filemd5("function-source.zip")
}

resource "google_cloudfunctions2_function" "csv_reports_processor" {
  name     = "csv-reports-processor"
  description = "Parses CSV files from GCS"
  project  = var.project_id
  location = var.region

  build_config {
    runtime     = "python310"
    entry_point = "process_gcs_file"
    source {
      storage_source {
        bucket     = google_storage_bucket.csv_reports_bucket.name
        object     = google_storage_bucket_object.function_source.name
        generation = google_storage_bucket_object.function_source.generation
      }
    }
  }

  service_config {
    available_memory    = "512M"
    timeout_seconds     = 600
    service_account_email = "investflow-function@${var.project_id}.iam.gserviceaccount.com"
    environment_variables = {
      BUCKET_NAME = google_storage_bucket.csv_reports_bucket.name
      SLACK_WEBHOOK_URL = var.slack_webhook_url
      SUPABASE_URL = var.supabase_url
      SUPABASE_API_KEY = var.supabase_api_key
    }
    max_instance_count = 3
    ingress_settings = "ALLOW_ALL"
    vpc_connector = null  # Explicitly set to null to use public internet
  }

  event_trigger {
    event_type = "google.cloud.storage.object.v1.finalized"
    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.csv_reports_bucket.name
    }
  }
}
