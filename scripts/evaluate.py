#!/usr/bin/env python

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array

def format_diff(diff, swap = False):
	if diff >= 0:
		return colored('+' + "{0:.0%}".format(diff), 'green' if swap == False else 'red')
	else:
		return colored("{0:.0%}".format(diff), 'red' if swap == False else 'green')

def calc_sum(operators):
	preparation_time = 0
	execution_time = 0
	for operator in operators:
		preparation_time += int(operator['preparation_time'])
		execution_time += int(operator['execution_time'])
	return (preparation_time, execution_time, preparation_time + execution_time)

if(len(sys.argv) != 3):
	print("Usage: " + sys.argv[0] + " benchmark1.json benchmark2.json")
	exit()

with open(sys.argv[1]) as old_file:
    old_data = json.load(old_file)

with open(sys.argv[2]) as new_file:
    new_data = json.load(new_file)

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

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
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
		description, old, new, low_is_good = row_value
		share = ("%.2f" % (float(new) / old * 100)) if old != 0 else ''
		diff = format_diff(float(new) / old - 1, low_is_good) if old != 0 else ''
		if not low_is_good:
			old = "%.6f" % old
			new = "%.6f" % new
		else:
			old = "{:,}".format(old)
			new = "{:,}".format(new)
		table_data.append([description, old, new, share, diff])

	for i in range(1, 6):
		table.justify_columns[i] = 'right'

	print(table.table)
	print("")
