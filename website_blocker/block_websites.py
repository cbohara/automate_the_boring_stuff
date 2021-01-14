HOST_PATH = '/etc/hosts'
REDIRECT = '127.0.0.1'
WEBSITES = [
    'www.netflix.com',
    'www.hulu.com'
]


def block_websites():
    print('Sleep hours')
    with open(HOST_PATH, 'r+') as file:
        content = file.read()
        for website in WEBSITES:
            if website in content:
                pass
            else:
                file.write(REDIRECT + ' ' + website + '\n')
    return


if __name__ == '__main__':
    block_websites()
