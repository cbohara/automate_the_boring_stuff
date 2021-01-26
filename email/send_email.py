import yagmail
import gspread


def get_emails(google_sheet_name, google_sheet_column, google_sheet_config):
    gc = gspread.service_account(google_sheet_config)
    sheet = gc.open(google_sheet_name).sheet1
    emails = [email for email in sheet.col_values(google_sheet_column) if email and email != 'Email']
    return emails


def send_emails(emails, sender, password, subject, body, attachment):
    body = body.replace('\\n', '\n')
    yag = yagmail.SMTP(sender, password)
    yag.send(
        to=sender,
        bcc=emails,
        subject=subject,
        contents=[body, yagmail.inline(attachment)]
    )


def execute(args):
    emails = get_emails(args.google_sheet_name, args.google_sheet_column, args.google_sheet_config)
    send_emails(emails, args.email_sender, args.email_password, args.email_subject, args.email_body, args.email_attachment)


if __name__ == '__main__':
    import configargparse
    from datetime import datetime

    config_parser = configargparse.ArgParser()
    config_parser.add('-c', '--config-file', required=True, is_config_file=True, help='config file path')

    args = config_parser.add_argument_group()
    args.add('--google_sheet_config', required=True, help='Config file to access google sheet')
    args.add('--google_sheet_name', required=True, help='Source google sheet containing emails')
    args.add('--google_sheet_column', required=True, help='Column in google sheet that contains emails')
    args.add('--email_sender', required=True, help='Email address of the email sender')
    args.add('--email_password', required=True, help='Email password of the email sender')
    args.add('--email_subject', required=True, help='Email subject')
    args.add('--email_body', required=True, help='String content of email body')
    args.add('--email_attachment', required=True, help='Path to additional attachment')

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'*******\n{timestamp}\n*******\n')
    config = config_parser.parse_args()
    for key, value in vars(config).items():
        print(f'{key} = {value}')

    execute(config)