#!/usr/bin/env python

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array

def str2bool(v):
	return v.lower() in ("yes", "true", "t", "1")

if str2bool(sys.argv[-1]):
	# draw charts
	import numpy as np
	import matplotlib.pyplot as plt

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
	print("Usage: " + sys.argv[0] + " benchmark1.json benchmark2.json")
	exit()

with open(sys.argv[1]) as old_file:
	old_data = json.load(old_file)

with open(sys.argv[2]) as new_file:
	new_data = json.load(new_file)

interpret = new_data
if (len(sys.argv) > 3):
	with open(sys.argv[3]) as file:
		interpret = json.load(file)
	if (str2bool(old_data['context']['jit']) or not str2bool(new_data['context']['jit']) or str2bool(new_data['context']['interpret']) or not str2bool(interpret['context']['jit']) or not str2bool(interpret['context']['interpret'])):
		print("Usage: " + sys.argv[0] + " opossum.json jit.json interpret.json")
		exit()

def print_operators(query, context, description):
	print(description + ' query: ' + query['name'] + ', Jit: ' + context['jit'] + ', lazy load: ' + context['lazy_load'] + ', Jit validate: ' + context['jit_validate'])
	table_data = []
	table_data.append(['Operator          ', 'Prepare', 'Execution', 'Total time', 'share Prepare (%)', 'share Execution (%)', 'share Total (%)'])
	prepare, execute, total = calc_sum(query['operators'])
	for row in query['operators']:
		row_prepare = int(row['preparation_time'])
		row_execute = int(row['execution_time'])
		row_total = row_prepare + row_execute
		table_data.append([row['name'], "{:,}".format(row_prepare), "{:,}".format(row_execute), "{:,}".format(row_total),
			"%.1f" % ((float(row_prepare)/prepare * 100) if prepare != 0 else 0.), "%.1f" % (float(row_execute)/execute* 100), "%.1f" % (float(row_total)/total*100) ])
	table_data.append(['Total', "{:,}".format(prepare), "{:,}".format(execute), "{:,}".format(total), '', '', ''])
	table = AsciiTable(table_data)
	for i in range(1, 7):
		table.justify_columns[i] = 'right'

	print(table.table)
	print("")

for old, new, interpret in zip(old_data['benchmarks'], new_data['benchmarks'], interpret['benchmarks']):
	print_operators(old, old_data['context'], 'Old')
	print_operators(new, new_data['context'], 'New')

	print('Query: ' + old['name'])
	table_data = []
	table_data.append(['Time (micro s)', 'Old', 'New', 'Share (%)', 'Diff %'])
	old_prepare, old_execute, old_total = calc_sum(old['operators'])
	new_prepare, new_execute, new_total = calc_sum(new['operators'])
	rows = [
		("items / s", float(old['items_per_second']), float(new['items_per_second']), False),
		("avg iteration", int(old['avg_real_time_per_iteration'])/1000, int(new['avg_real_time_per_iteration'])/1000, True),
		("prepare operators", old_prepare, new_prepare, True),
		("execute operators", old_execute, new_execute, True),
		("total operators", old_total, new_total, True)
	]
	table = AsciiTable(table_data)
	for row_value in rows:
		description, _old, _new, low_is_good = row_value
		share = ("%.2f" % (float(_new) / _old * 100)) if _old != 0 else ''
		diff = format_diff(float(_new) / _old - 1, low_is_good) if _old != 0 else ''
		if not low_is_good:
			_old = "%.6f" % _old
			_new = "%.6f" % _new
		else:
			_old = "{:,}".format(_old)
			_new = "{:,}".format(_new)
		table_data.append([description, _old, _new, share, diff])

	for i in range(1, 6):
		table.justify_columns[i] = 'right'

	print(table.table)

	if len(sys.argv) > 4 and str2bool(sys.argv[-1]):
		jit_prepare, jit_execute, jit_total = calc_sum(new['operators'], True)
		interpret_prepare, interpret_execute, interpret_total = calc_sum(interpret['operators'], True)
		opossum_total_pipeline = int(old['avg_real_time_per_iteration']) / 1000000000.
		jit_total_pipeline = int(new['avg_real_time_per_iteration']) / 1000000000.
		interpreted_total_pipeline = int(interpret['avg_real_time_per_iteration']) / 1000000000.

		print("Generating diagram (interpeated pipeline: "+ "{:,.0f}".format(interpret_execute) + ")")
		N = 3
		common_time = jit_total_pipeline - jit_total / 1000000.
		hyrise_only = (opossum_total_pipeline, 0, 0)
		pipeline_execition = (0, interpreted_total_pipeline, jit_total_pipeline)
		pipeline_spez = (0, 0, jit_prepare / 1000000. + common_time)
		common_operators = (common_time, common_time, common_time)
		ind = np.arange(N)    # the x locations for the groups
		width = 0.35       # the width of the bars: can also be len(x) sequence

		plt.rcParams.update({'font.size': 16})
		plt.rcParams.update({'figure.figsize': [6.5, 6]})
		p1 = plt.bar(ind, hyrise_only, color='#9D56C2')
		p2 = plt.bar(ind, pipeline_execition, color='#DC002D')
		p3 = plt.bar(ind, pipeline_spez, color='#00A739')
		p4 = plt.bar(ind, common_operators, color='#F87A12')

		plt.ylabel('Runtime (s)')
		plt.title(old['name'])
		plt.xticks(ind, ('Hyrise', 'Interpreted', 'Specialized'))
		#plt.yticks(np.arange(0, 81, 10))
		if old['name'] == 'TPC-H ' + sys.argv[-2]:
			plt.legend((p1[0], p2[0], p3[0], p4[0]), ('Hyrise Only Operators', 'Pipeline Execution', 'Pipeline Specialization', 'Common Operators'))

		#plt.show()
		plt.savefig(old['name'].replace(' ', '_') + '.svg', transparent=True)
		plt.gcf().clear()

	print("")
