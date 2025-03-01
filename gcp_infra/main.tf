resource "google_storage_bucket" "data_bucket" {
  name          = "${var.project_id}-data-bucket"
  location      = var.region
  storage_class = "STANDARD"
}

resource "google_bigquery_dataset" "data_dataset" {
  dataset_id = "justdone_analytics"
  project    = var.project_id
  location   = var.region
}