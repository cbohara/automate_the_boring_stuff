import psutil


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
	kill_chrome()
