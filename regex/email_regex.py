import sys
import pyperclip
import re


def compile_email_regex():
    """Compile regex for email addresses."""
    return re.compile(r'''(
        [a-zA-Z0-9._%+-]+                   # username
        @
        [a-zA-Z0-9._%+-]+                   # domain name
        (\.[a-zA-Z]{2,4})                   # dot something
    )''', re.VERBOSE)


def find_email(text, email_regex):
    """Returns list of emails found on OS clipboard."""
    # store matches
    matches = []

    # find all emails
    for groups in email_regex.findall(text):
        # find and append email
        matches.append(groups[0])

    return matches


def print_output(matches, message):
    if len(matches) > 0:
        pyperclip.copy('\n'.join(matches))
        print('Copied ' + message + ' to clipboard:')
        print('\n'.join(matches))
    else:
        print('No ' + message + ' found in clipboard.')


def main(script):
    """Finds phone#/emails from text on OS clipboard and returns only phone#/emails to clipboard."""
    # compile regex
    email_regex = compile_email_regex()

    # paste text from OS clipboard 
    text = str(pyperclip.paste())

    # list of found emails in OS clipboard text
    email_matches = find_email(text, email_regex)

    # join matches into a string to put on the OS clipboard
    print_output(email_matches, message='email addresses')


if __name__ == "__main__":
    sys.exit(main(sys.argv))
