Running actual budget as desktop app   
Automate backup budget to S3 for safekeeping   

```bash
python -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

Variables set in .env file
```bash
ACTUAL_BASE_URL =
ACTUAL_PASSWORD =
ACTUAL_BUDGET_FILE =
S3_BUCKET =
S3_PREFIX =
AWS_REGION = 
```

Running as cron job

```bash
0 2 * * * cd /Users/charlie/git/automate_the_boring_stuff/backup_budget/ && /Users/charlie/git/automate_the_boring_stuff/backup_budget/venv/bin/python3 /Users/charlie/git/automate_the_boring_stuff/backup_budget/backup.py >> /Users/charlie/git/automate_the_boring_stuff/backup_budget/backup.log 2>&1
```