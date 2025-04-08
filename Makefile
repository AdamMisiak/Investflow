# --------- CONFIG ---------
PROJECT_ID     ?= your-gcp-project-id
BUCKET_NAME    ?= investflow-bucket
REGION         ?= europe-central2

ZIP_NAME       := function-source.zip
FUNCTION_DIR   := cloud_function

# --------- COMMANDS ---------

.PHONY: all build deploy clean tf-init tf-apply tf-destroy

all: build deploy

# Create zip from cloud function source
build:
	cd $(FUNCTION_DIR) && zip -r ../$(ZIP_NAME) .

# Initialize Terraform
tf-init:
	terraform init

# Apply Terraform with vars
tf-apply:
	terraform apply -var="project_id=$(PROJECT_ID)" -var="bucket_name=$(BUCKET_NAME)" -var="region=$(REGION)"

# Destroy infra
tf-destroy:
	terraform destroy -var="project_id=$(PROJECT_ID)" -var="bucket_name=$(BUCKET_NAME)" -var="region=$(REGION)"

# Full deploy: build → init → apply
deploy: build tf-init tf-apply

# Clean generated zip
clean:
	rm -f $(ZIP_NAME)
