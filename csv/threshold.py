#!/usr/local/bin/python3

import sys

header = ""
for line in sys.stdin:
    header += line

header_list = header.split("|")
print(header_list)
