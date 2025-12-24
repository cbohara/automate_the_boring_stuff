from actual import Actual
from datetime import datetime
import boto3
import os
import tempfile
from pathlib import Path

# Configuration
ACTUAL_BASE_URL = "http://localhost:5007"
ACTUAL_PASSWORD = "xzv-xjg2gbe_QKG9gjx"  # Your Actual password (if set)
ACTUAL_BUDGET_FILE = "FI"  # Your budget file name
S3_BUCKET = "charlie-ohara-actual-budget-backup"
S3_PREFIX = "backups/"
AWS_REGION = "us-east-1"

def main():
    # Generate export filename
    date_str = datetime.now().strftime("%Y-%m-%d")
    export_filename = f"{date_str}-{ACTUAL_BUDGET_FILE}-verify.zip"

    with tempfile.TemporaryDirectory() as temp_dir:
        export_path = os.path.join(temp_dir, export_filename)
        print(f"Exporting {ACTUAL_BUDGET_FILE} to {export_path}")
    
    # Connect to Actual and export
        with Actual(base_url=ACTUAL_BASE_URL, password=ACTUAL_PASSWORD, file=ACTUAL_BUDGET_FILE) as actual:
            # Export the budget
            actual.export_data(export_path)
            print(f"✓ Exported to {export_path}")
    
        # Upload to S3
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        s3_client.upload_file(export_path, S3_BUCKET, f"{S3_PREFIX}{export_filename}")
        print(f"✓ Uploaded to s3://{S3_BUCKET}/{S3_PREFIX}{export_filename}")

if __name__ == "__main__":
    main()