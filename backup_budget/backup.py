from actual import Actual
from datetime import datetime
import boto3
import os
import tempfile
from pathlib import Path
from dotenv import load_dotenv


load_dotenv(dotenv_path='.env')
ACTUAL_BASE_URL = os.getenv('ACTUAL_BASE_URL')
ACTUAL_PASSWORD = os.getenv('ACTUAL_PASSWORD')
ACTUAL_BUDGET_FILE = os.getenv('ACTUAL_BUDGET_FILE')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = os.getenv('S3_PREFIX')
AWS_REGION = os.getenv('AWS_REGION')

def main():
    # Generate export filename
    timestamp = datetime.now()
    date_str = timestamp.strftime("%Y-%m-%d")
    export_filename = f"{date_str}-{ACTUAL_BUDGET_FILE}.zip"

    with tempfile.TemporaryDirectory() as temp_dir:
        export_path = os.path.join(temp_dir, export_filename)
    
    # Connect to Actual and export
        with Actual(base_url=ACTUAL_BASE_URL, password=ACTUAL_PASSWORD, file=ACTUAL_BUDGET_FILE) as actual:
            # Export the budget
            actual.export_data(export_path)
    
        # Upload to S3
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        s3_client.upload_file(export_path, S3_BUCKET, f"{S3_PREFIX}{export_filename}")
        print(f"{timestamp} - s3://{S3_BUCKET}/{S3_PREFIX}{export_filename}")

if __name__ == "__main__":
    main()