# Investflow CSV Reports Processor

A Cloud Function that processes CSV reports from GCS and sends notifications to Slack.

## Overview

This project consists of:
- A Cloud Function (2nd gen) that processes CSV files
- GCS bucket for storing reports
- Slack integration for notifications
- Terraform for infrastructure management

## Prerequisites

- Google Cloud Project with billing enabled
- Terraform installed
- gcloud CLI installed and authenticated
- Slack webhook URL

## Project Structure

```
.
├── cloud_function/          # Cloud Function source code
│   ├── main.py             # Main function code
│   ├── requirements.txt    # Python dependencies
│   └── ...                 # Other function files
├── infrastructure/         # Terraform configuration
│   ├── main.tf            # Main infrastructure
│   ├── variables.tf       # Terraform variables
│   └── outputs.tf         # Terraform outputs
└── Makefile               # Build and deployment automation
```

## Setup

1. Create a `.env` file in the root directory:
```bash
SLACK_WEBHOOK_URL=your_slack_webhook_url
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_api_key
CSV_FILE=path/to/your/test.csv  # Only for local testing
```

2. Initialize Terraform:
```bash
cd infrastructure
terraform init
```

## Deployment

The project uses a Makefile for easy deployment. Available commands:

```bash
# Build the function package
make build

# Deploy everything (build + terraform apply)
make deploy

# Show planned infrastructure changes
make plan

# Clean up resources
make clean

# Show help
make help
```

## How It Works

1. CSV files are uploaded to the GCS bucket
2. Cloud Function is triggered by the upload event
3. Function processes the CSV file:
   - Parses stock and options trades
   - Updates records in the database
   - Sends Slack notification with processing results

## Local Testing

1. Set up local environment:
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
cd cloud_function
pip install -r requirements.txt
```

2. Add test CSV file path to `.env`:
```bash
CSV_FILE=path/to/your/test.csv
```

3. Run the function locally:
```bash
cd cloud_function
python main.py
```

## Infrastructure

The Terraform configuration sets up:
- GCS bucket for CSV reports
- Cloud Function (2nd gen)
- Required IAM permissions
- Event trigger for new file uploads

## Environment Variables

Required environment variables in `.env`:
```bash
SLACK_WEBHOOK_URL=your_slack_webhook_url
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_api_key
CSV_FILE=path/to/your/test.csv  # Only for local testing
```

These variables are used by:
- Cloud Function: For Slack notifications and database access
- Terraform: For setting up the infrastructure
- Local testing: For running the function locally

