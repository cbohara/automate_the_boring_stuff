#!/usr/bin/env python3
import os

cwd = os.path.abspath(os.path.dirname(__file__))
rel_path = "../data/verizon_data_201810.txt"
data_path = os.path.abspath(os.path.join(cwd, rel_path))

usage_amounts = []
with open(data_path) as f:
	for line in f:
		if line.strip().endswith("MB"):
			mb_usage = float(line.strip().replace("MB", ""))
			usage_amounts.append(mb_usage)

total_mb_usage = sum(usage_amounts)
print(total_mb_usage)
total_gb_usage = total_mb_usage/1000
print(total_gb_usage)
