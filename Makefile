# --------- CONFIG ---------
PROJECT_ID     ?= investflow
BUCKET_NAME    ?= investflow-csv-reports-bucket
REGION         ?= europe-central2
SLACK_WEBHOOK_URL ?= $(shell cat .env | grep SLACK_WEBHOOK_URL | cut -d '=' -f2)

ZIP_NAME       := function-source.zip
FUNCTION_DIR   := cloud_function
INFRA_DIR      := infrastructure

# --------- COMMANDS ---------

.PHONY: all build deploy clean test plan fmt help

all: build deploy

# Create zip from cloud function source
build:
	@echo "Building function package..."
	cd $(FUNCTION_DIR) && \
	zip -r ../$(ZIP_NAME) . -x "*.pyc" "*.pyo" "*.pyd" "__pycache__/*" "*.git*" "*.env*" "*.DS_Store"

# Initialize Terraform
tf-init:
	terraform init

# Apply Terraform with vars
tf-apply:
	terraform apply -var="project_id=$(PROJECT_ID)" -var="bucket_name=$(BUCKET_NAME)" -var="region=$(REGION)" -var="slack_webhook_url=$(SLACK_WEBHOOK_URL)"

# Destroy infra
tf-destroy:
	terraform destroy -var="project_id=$(PROJECT_ID)" -var="bucket_name=$(BUCKET_NAME)" -var="region=$(REGION)" -var="slack_webhook_url=$(SLACK_WEBHOOK_URL)"

# Full deploy: build → init → apply
deploy: build
	@echo "Deploying function..."
	cd $(INFRA_DIR) && \
	terraform init && \
	terraform apply -auto-approve \
		-var="project_id=$(PROJECT_ID)" \
		-var="bucket_name=$(BUCKET_NAME)" \
		-var="region=$(REGION)" \
		-var="slack_webhook_url=$(SLACK_WEBHOOK_URL)"

# Clean generated zip
clean:
	@echo "Cleaning up..."
	rm -f $(ZIP_NAME)
	cd $(INFRA_DIR) && terraform destroy -auto-approve \
		-var="project_id=$(PROJECT_ID)" \
		-var="bucket_name=$(BUCKET_NAME)" \
		-var="region=$(REGION)" \
		-var="slack_webhook_url=$(SLACK_WEBHOOK_URL)"

# Helper targets
plan:
	@echo "Planning infrastructure changes..."
	cd $(INFRA_DIR) && \
	terraform plan \
		-var="project_id=$(PROJECT_ID)" \
		-var="bucket_name=$(BUCKET_NAME)" \
		-var="region=$(REGION)" \
		-var="slack_webhook_url=$(SLACK_WEBHOOK_URL)"

help:
	@echo "Available targets:"
	@echo "  build    - Build the function package"
	@echo "  deploy   - Deploy the function (build + terraform apply)"
	@echo "  clean    - Clean up resources (destroy infrastructure)"
	@echo "  plan     - Show planned infrastructure changes"
	@echo "  help     - Show this help message"
