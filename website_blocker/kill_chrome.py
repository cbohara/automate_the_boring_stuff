import appscript
import psutil
import configargparse
from datetime import datetime


def get_all_open_tabs():
	all_tabs = set()
	for index in range(100):
		try:
			current_tabs = list(map(lambda x: x.title(), appscript.app('Google Chrome').windows[index].tabs()))
		except appscript.reference.CommandError as e:
			print('Done checking all windows for open tabs')
			break
		else:
			all_tabs.update(current_tabs)
	return all_tabs


def get_processes(process_name):
	processes = []
	for process in psutil.process_iter():
		try:
			process_info = process.as_dict(attrs=['pid', 'name', 'create_time', 'memory_percent', 'cpu_times'])
			if process_name.lower() in process_info['name'].lower():
				processes.append(process_info)
		except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
			print(f'No process found: {e}')
			pass
	return processes


def kill_process(process):
	print(f'Killing {process["pid"]}')
	process = psutil.Process(process['pid'])
	process.terminate()


def kill_chrome():
	process_list = get_processes('chrome')
	for process in process_list:
		try:
			kill_process(process)
		except Exception as e:
			print(f'Unable to kill process {process}: {e}')
			pass


if __name__ == '__main__':
	config_parser = configargparse.ArgParser()
	config_parser.add('-c', '--config-file', required=True, is_config_file=True, help='config file path')

	args = config_parser.add_argument_group()
	args.add('--websites', required=True, help='Websites to check')

	timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	print(f'*******\n{timestamp}\nkill_chrome.py\n*******')
	config = config_parser.parse_args()
	for key, value in vars(config).items():
		print(f'{key} = {value}')

	all_open_tabs = get_all_open_tabs()
	print(f'all_open_tabs: {all_open_tabs}')

	websites_to_block = set(f'www.{website.strip()}.com' for website in config.websites.split(','))
	print(f'websites_to_block: {websites_to_block}')

	watching_blocked_website = (all_open_tabs & websites_to_block)
	print(f'watching_blocked_website: {watching_blocked_website}')

	if watching_blocked_website:
		print('Killing chrome')
		kill_chrome()
	else:
		print('Not killing chrome')