HOST_PATH = '/etc/hosts'
REDIRECT = '0.0.0.0'
WEBSITES =[
    'www.netflix.com',
    'www.hulu.com'
]


def enable_websites():
    print('TV hours')
    with open(HOST_PATH, 'r+') as file:
        content = file.readlines()
        file.seek(0)
        for line in content:
            if not any(website in line for website in WEBSITES):
                file.write(line)
        file.truncate()


if __name__ == '__main__':
    enable_websites()
