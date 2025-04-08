output "bucket_name" {
  description = "Name of the storage bucket"
  value       = google_storage_bucket.csv_reports_bucket.name
}

output "function_name" {
  description = "Name of the deployed cloud function"
  value       = google_cloudfunctions_function.csv_reports_processor.name
}
