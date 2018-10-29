#!/usr/bin/env python

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array

p_value_significance_threshold = 0.001
min_iterations = 10
min_runtime_ns = 60 * 1000 * 1000 * 1000

def format_diff(diff, swap = False):
	if diff >= 0:
		return colored('+' + "{0:.0%}".format(diff), 'green' if swap == False else 'red')
	else:
		return colored("{0:.0%}".format(diff), 'red' if swap == False else 'green')

def combine_operators(operators):
	spez_time = 0
	exec_time = 0
	for operator in operators:
		spez_time += int(operator['preparation_time'])
		exec_time += int(operator['execution_time'])
	return (spez_time, exec_time, spez_time + exec_time)

def calculate_and_format_p_value(old, new):
	p_value = ttest_ind(array('d', old['iteration_durations']), array('d', new['iteration_durations']))[1]
	is_significant = p_value < p_value_significance_threshold

	notes = ""
	old_runtime = sum(runtime for runtime in old['iteration_durations'])
	new_runtime = sum(runtime for runtime in new['iteration_durations'])
	if (old_runtime < min_runtime_ns or new_runtime < min_runtime_ns):
		is_significant = False
		notes += "(run time too short) "

	if (len(old['iteration_durations']) < min_iterations or len(new['iteration_durations']) < min_iterations):
		is_significant = False
		notes += "(not enough runs) "

	color = 'green' if is_significant else 'grey'
	return colored(notes + "{0:.4f}".format(p_value), color)


if(len(sys.argv) != 3):
	print("Usage: " + sys.argv[0] + " benchmark1.json benchmark2.json")
	exit()

with open(sys.argv[1]) as old_file:
    old_data = json.load(old_file)

with open(sys.argv[2]) as new_file:
    new_data = json.load(new_file)

has_operator_times = 'operators' in old_data['benchmarks'][0] and 'operators' in new_data['benchmarks'][0]
table_data = []
header = ["Benchmark", "prev. iter/s", "runs", "new iter/s", "runs", "change", "p-value (significant if <" + str(p_value_significance_threshold) + ")"]
if has_operator_times:
	header += ["prev. spec", "new spec", "prev. exec", "new exec", "% spez prev.", "% spez new", "change spec", "change exec"]
table_data.append(header)

average_diff_sum = 0.0

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
	if old['name'] != new['name']:
		print("Benchmark name mismatch")
		exit()
	if float(old['items_per_second']) > 0.0:
		diff = float(new['items_per_second']) / float(old['items_per_second']) - 1
		average_diff_sum += diff
	else:
		diff = float('nan')

	diff_formatted = format_diff(diff)
	p_value_formatted = calculate_and_format_p_value(old, new)

	row = [old['name'], "%.6f" % float(old['items_per_second']), str(old['iterations']), "%.6f" % float(new['items_per_second']), str(new['iterations']), diff_formatted, p_value_formatted]
	if has_operator_times:
		old_spez, old_exec, old_total = combine_operators(old['operators'])
		new_spez, new_exec, new_total = combine_operators(new['operators'])
		diff_spez = format_diff(float(new_spez) / float(old_spez) - 1, True) if old_spez != 0 else ''
		diff_exec = format_diff(float(new_exec) / float(old_exec) - 1, True) if old_exec != 0 else ''
		row += ["{:,}".format(old_spez), "{:,}".format(new_spez), "{:,}".format(old_exec), "{:,}".format(new_exec), "{0:.0%}".format(float(old_spez) / old_total), "{0:.0%}".format(float(new_spez) / new_total), diff_spez, diff_exec]

	table_data.append(row)

table_data.append(['average', '', '', '', '', format_diff(average_diff_sum / len(old_data['benchmarks'])), ''])

table = AsciiTable(table_data)
for i in range(1, 15 if has_operator_times else 7):
	table.justify_columns[i] = 'right'

print("")
print(table.table)
print("")
