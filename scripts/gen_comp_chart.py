#!/usr/bin/env python

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array
import numpy as np
import matplotlib.pyplot as plt

def str2bool(v):
	return v.lower() in ("yes", "true", "t", "1")

def format_diff(diff, swap = False):
	if diff >= 0:
		return colored('+' + "{0:.0%}".format(diff), 'green' if swap == False else 'red')
	else:
		return colored("{0:.0%}".format(diff), 'red' if swap == False else 'green')

def calc_sum(operators, only_jit_wrapper = False):
	preparation_time = 0
	execution_time = 0
	for operator in operators:
		if only_jit_wrapper and operator['name'] != 'JitOperatorWrapper':
			continue
		preparation_time += int(operator['preparation_time'])
		execution_time += int(operator['execution_time'])
	return (preparation_time, execution_time, preparation_time + execution_time)

if (len(sys.argv) < 3):
	print("Usage: " + sys.argv[0] + " <result dir 1> <result dir 2>")
	exit()

files = ['result_wo_jit_wo_mvcc.json', 'result_w_interpret_jit_wo_mvcc.json', 'result_w_jit_wo_mvcc.json']
folders = [sys.argv[1], sys.argv[2]]
data = []
for folder in folders:
	if folder[-1] != '/':
		folder += '/'
	folder_data = []
	for file_name in files:
		with open(folder + file_name) as file:
			folder_data.append(json.load(file))
	data.append(folder_data)

def calc(data, id):
	prepare, execute, total = calc_sum(data[2]['benchmarks'][id]['operators'])
	interpret_prepare, interpret_execute, interpret_total = calc_sum(data[1]['benchmarks'][id]['operators'], True)
	jit_prepare, jit_execute, jit_total = calc_sum(data[2]['benchmarks'][id]['operators'], True)

	interpreted_total_pipeline = int(data[1]['benchmarks'][id]['avg_real_time_per_iteration']) / 1000000000.
	jit_total_pipeline = int(data[2]['benchmarks'][id]['avg_real_time_per_iteration']) / 1000000000.
	common_time = jit_total_pipeline - jit_total / 1000000.

	return (common_time, interpret_execute, jit_prepare, jit_execute, interpreted_total_pipeline, jit_total_pipeline)

for idx, query in enumerate(data[0][0]['benchmarks']):
	print('Query: ' + query['name'])
	opossum_total_pipeline = int(query['avg_real_time_per_iteration']) / 1000000000.

	common_time_l, interpret_execute_l, jit_prepare_l, jit_execute_l, interpreted_total_pipeline_l, jit_total_pipeline_l = calc(data[0], idx)
	common_time_r, interpret_execute_r, jit_prepare_r, jit_execute_r, interpreted_total_pipeline_r, jit_total_pipeline_r = calc(data[1], idx)

	hyrise_only = (opossum_total_pipeline, 0, 0, 0, 0)
	pipeline_execition = (0, interpreted_total_pipeline_l, interpreted_total_pipeline_r, jit_total_pipeline_l, jit_total_pipeline_r)
	pipeline_spez = (0, 0, 0, jit_prepare_l / 1000000. + common_time_l, jit_prepare_r / 1000000. + common_time_r)
	common_operators = (common_time_l, common_time_l, common_time_l, common_time_l, common_time_l)

	ind = np.arange(5)
	plt.rcParams.update({'font.size': 16})
	plt.rcParams.update({'figure.figsize': [10.5, 6]})
	p1 = plt.bar(ind, hyrise_only, color='#9D56C2')
	p2 = plt.bar(ind, pipeline_execition, color='#DC002D')
	p3 = plt.bar(ind, pipeline_spez, color='#00A739')
	p4 = plt.bar(ind, common_operators, color='#F87A12')

	plt.ylabel('Runtime (s)')
	plt.title(query['name'])
	plt.xticks(ind, ('Hyrise', 'Interpreted\nLambdas', 'Interpreted\nFunc Objects', 'Specialized\nLambdas', 'Specialized\nFunc Objects'))
	if query['name'] == 'TPC-H ' + sys.argv[-1]:
		plt.legend((p1[0], p2[0], p3[0], p4[0]), ('Hyrise Only Operators', 'Pipeline Execution', 'Pipeline Specialization', 'Common Operators'))
	ax = plt.axes()
	ax.yaxis.grid()
	#plt.show()
	plt.savefig(query['name'].replace(' ', '_') + '.svg', transparent=True)
	plt.gcf().clear()
