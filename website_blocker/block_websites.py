import configargparse
from datetime import datetime


def block_websites(config):
    websites = [f'www.{website.strip()}.com' for website in config.websites.split(',')]
    with open(config.host_path, 'r+') as file:
        content = file.read()
        for website in websites:
            if website in content:
                pass
            else:
                file.write(config.redirect + ' ' + website + '\n')
    return


if __name__ == '__main__':
    config_parser = configargparse.ArgParser()
    config_parser.add('-c', '--config-file', required=True, is_config_file=True, help='config file path')

    args = config_parser.add_argument_group()
    args.add('--host_path', default='/etc/hosts', help='path to operating system file which maps hostnames to IP addresses')
    args.add('--redirect', default='127.0.0.1', help='localhost IP address to redirect to')
    args.add('--websites', required=True, help='Websites to block')

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'*******\n{timestamp}\nblock_websites.py\n*******')
    config = config_parser.parse_args()
    for key, value in vars(config).items():
        print(f'{key} = {value}')
    block_websites(config)