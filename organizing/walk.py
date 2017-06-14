import os

for folder, subfolders, filenames in os.walk('/Users/cbohara/code/automate'):
    print('The current folder is ' + folder)

    for subfolder in subfolders:
        print("SUBFOLDER INSIDE " + folder + ": " + subfolder)

    for filename in filenames:
        print("FILE INSIDE " + folder + ": " + filename)

    print('')
