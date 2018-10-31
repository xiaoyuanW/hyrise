#!/usr/bin/env python
# for jit evaluate
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

def floatToString(inputValue):
    return ('%.15f' % inputValue).rstrip('0').rstrip('.')

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
		if (operator['prepare']):
			preparation_time += int(operator['walltime'])
		else:
			execution_time += int(operator['walltime'])
	return (preparation_time, execution_time, preparation_time + execution_time)

def combine(results):
	result = {}
	total_pipeline = 0
	total_execute = 0
	jit_prepare = 0
	jit_execute = 0
	for data in results:
		total_pipeline += data['pipeline_execution_time'] + data['pipeline_compile_time'] + data['pipeline_optimize_time']
		total_execute += data['pipeline_execution_time']
		result = calc_sum(data['operators'], True)
		jit_prepare += result[0]
		jit_execute += result[1]
	total_pipeline = total_pipeline / len(results) / 1000000.
	total_execute = total_execute / len(results) / 1000000.
	jit_prepare = jit_prepare / len(results) / 1000000.
	jit_execute = jit_execute / len(results) / 1000000.
	return {
		"total_pipeline": total_pipeline,
		"total_execute": total_execute,
		"jit_prepare": jit_prepare,
		"jit_execute": jit_execute,
	}

if (len(sys.argv) < 2):
	print("Usage: " + sys.argv[0] + " <json file>")
	exit()

def summarize(benchmark_data):
	result = {}
	for data in benchmark_data['results']:
		engine = 'custom' if 'hand_written' in data['experiment'] and data['experiment']['hand_written'] else data['experiment']['engine']
		if engine not in result:
			result[engine] = {}
		result[engine][floatToString(data['experiment']['scale_factor'])] = combine(data['results'])
	return result

file_name = sys.argv[1]
labels = ['Hyrise', 'Jit', 'Custom']
summarized_data = {}
with open(file_name) as file:
	summarized_data = summarize(json.load(file))

data_series = {}
const_data = {}
for key, summarized in summarized_data.iteritems():
	current_series = []
	const_summary = []
	for scale_factor in range(1, 21):
		id = floatToString(scale_factor/10.)
		const = summarized[id]['total_pipeline'] - summarized[id]['total_execute'] + summarized[id]['jit_prepare']
		current_series.append(summarized[id]['total_pipeline']) # -const
		const_summary.append(const)
	data_series[key] = current_series
	const_data[key] = const_summary
plot_horizontal_lines = True
ind = []
for i in range(1, 21):
	ind.append(i/10.)
plt.rcParams.update({'font.size': 16})
plt.rcParams.update({'figure.figsize': [10.5, 6 * 1.5]})
p1 = plt.plot(ind, data_series['opossum'], color='#9D56C2')
plt.plot(ind, data_series['opossum'], 'x', color='#9D56C2')
p2 = plt.plot(ind, data_series['jit'], color='#DC002D')
plt.plot(ind, data_series['jit'], 'x', color='#DC002D')
p3 = plt.plot(ind, data_series['custom'], color='#00A739')
plt.plot(ind, data_series['custom'], 'x', color='#00A739')
if plot_horizontal_lines:
	plt.axhline(y=const_data['opossum'][0], linestyle='-', color='#9D56C2')
	plt.axhline(y=const_data['jit'][0], linestyle='-', color='#DC002D')
	plt.axhline(y=const_data['custom'][0], linestyle='-.', color='#00A739')

plt.ylabel('Runtime (s)')
plt.xlabel('Scale Factor')
plt.title('Table scan with one predicate')
plt.xticks(ind[1::2], ind[1::2])
if str2bool(sys.argv[-1]):
	plt.legend((p2[0], p3[0], p1[0]), (labels[1], labels[2], labels[0]))
ax = plt.axes()
ax.yaxis.grid()
plt.ylim(0, 0.042)
#plt.show()
filename = 'tablescan_w_different_scales.svg'
plt.savefig(filename, transparent=True)
print('Generated graph ' + filename)
plt.gcf().clear()
