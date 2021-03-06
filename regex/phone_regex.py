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


def find_phone(text, phone_regex):
    """Returns list of phone# found on OS clipboard."""
    # store matches
    matches = []

    # find all phone#
    for groups in phone_regex.findall(text):
        # take into account phone numbers that do not have area code
        if groups[1] == '':
            phone = '-'.join([groups[3], groups[5]])
        else:
            phone = '-'.join([groups[1], groups[3], groups[5]])
        # add extension if provided
        if groups[8] != '':
            phone += ' x' + groups[8]
        # append phone number to master list
        matches.append(phone)

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
    phone_regex = compile_phone_regex()

    # paste text from OS clipboard 
    text = str(pyperclip.paste())

    # list of found phone# in OS clipboard text
    phone_matches = find_phone(text, phone_regex)

    # join matches into a string to put on the OS clipboard
    print_output(phone_matches, message='phone numbers')


if __name__ == "__main__":
    sys.exit(main(sys.argv))
