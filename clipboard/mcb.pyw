#!/usr/local/bin/python3
# mcb.pyw - Saves and loads pieces of text to the clipboard
# Usage: 
#   py.exe mcb.pyw [keyword] - saves clipboard text to keyword
#   py.exe mcb.pyw [keyword] - loads keyword content to the clipboard
#   py.exe mcb.pyw list - loads all keywords to the clipboard

import sys
import shelve
import pyperclip

mcb_shelf = shelve.open('mcb')


