import sys
import pyperclip
import re


def compile_phone_regex():
    """Compile regex for phone numbers."""

    return re.compile(r'''(
        (\d{3}|\(\d{3}\))?                  # area code
        (\s|-|\.)?                          # separator
        (\d{3})                             # first 3 digits
        (\s|-|\.)                           # separator
        (\d{4})                             # last 4 digits
        (\s*(ext|x|ext.)\s*(\d{2,5}))?      # extension
    )''', re.VERBOSE)


def compile_email_regex():
    """Compile regex for email addresses."""

    return re.compile(r'''(
        [a-zA-Z0-9._%+-]+                   # username
        @
        [a-zA-Z0-9._%+-]+                   # domain name
        (\.[a-zA-Z]{2,4})                   # dot something
    )''', re.VERBOSE)


def find_phone(text, phone_regex):
    """Returns list of phone# found on OS clipboard."""
    # store matches
    matches = []

    # find all phone#
    for groups in phone_regex.findall(text):
        # store phone number as ###-###-####
        phone = '-'.join([groups[1], groups[3], groups[5]])
        # add extension if provided
        if groups[8] != '':
            phone += ' x' + groups[8]
        # append phone number to master list
        matches.append(phone)

   return matches


def find_email(text, email_regex):
    """Returns list of emails found on OS clipboard."""
    # store matches
    matches = []

    # find all emails
    for groups in email_regex.findall(text):
        # find and append email
        matches.append(groups[0])

    return matches


def print_output(message, matches):
    if len(matches) > 0:
        pyperclip.copy('\n'.join(matches))
        print('Copied ' + message + ' to clipboard:')
        print('\n'.join(matches))
    else:
        print('No ' + message + ' found in clipboard.')


def main(script):
    """Finds phone#/emails from text on OS clipboard and returns only phone#/emails to clipboard."""
    # compile regex
    phone_regex = compile_phone_regex()
    email_regex = compile_email_regex()

    # paste text from OS clipboard 
    text = str(pyperclip.paste())

    # list of found phone# in OS clipboard text
    phone_matches = find_phone(text, phone_regex)
    # list of found emails in OS clipboard text
    email_matches = find_email(text, email_regex)

    # join matches into a string to put on the OS clipboard
    print_output(message='phone numbers', phone_matches)
    print_output(message='email addresses', email_matches)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
