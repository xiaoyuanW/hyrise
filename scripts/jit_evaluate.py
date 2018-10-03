#!/usr/bin/env python

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array

def str2bool(v):
	return v.lower() in ("yes", "true", "t", "1")

if len(sys.argv) > 2 and str2bool(sys.argv[2]):
	# draw charts
	import numpy as np
	import matplotlib.pyplot as plt

def combine_results(results):
	d = {}
	compile_time = 0
	execution_time = 0
	optimize_time = 0
	for result in results:
		compile_time += int(result['pipeline_compile_time'])
		execution_time += int(result['pipeline_execution_time'])
		optimize_time += int(result['pipeline_optimize_time'])
		for operator in result['operators']:
			if operator['name'] not in d:
				d[operator['name']] = {'prepare': 0, 'execute': 0}
			if (operator['prepare']):
				d[operator['name']]['prepare'] += float(operator['walltime'])
			else:
				d[operator['name']]['execute'] += float(operator['walltime'])
	combined_rumtimes = []
	for key, value in d.iteritems():
		combined_rumtimes.append({'name': key, 'prepare': value['prepare'] / len(results), 'execute': value['execute'] / len(results)})
	return (combined_rumtimes, (compile_time/len(results), execution_time/len(results), optimize_time/len(results), (compile_time + execution_time + optimize_time)/len(results)))

def calc_sum(operators, only_jit_wrapper = False):
	prepare = 0
	execute = 0
	for operator in operators:
		if operator['name'].startswith('_'):
			continue
		if only_jit_wrapper and operator['name'] != 'JitOperatorWrapper':
			continue
		prepare += operator['prepare']
		execute += operator['execute']
	return (prepare, execute)

with open(sys.argv[1]) as old_file:
    data = json.load(old_file)

d = {}

for experiment in data['results']:
	if experiment['experiment']['task'] != 'run':
		continue
	total_pipeline = int()
	summary, runtimes = combine_results(experiment['results'])
	summary.sort(key=lambda x: x['name'])
	compile_time, execution_time, optimize_time, total_time = runtimes
	print('Query: ' + experiment['experiment']['query_id'] + ', Engine: ' + experiment['experiment']['engine'] + (', Specialize: ' + str(experiment['experiment']['jit_use_jit']) if 'jit_use_jit' in experiment['experiment'] else ''))
	print('compile time: %.0f' % compile_time+ ', execution time: %.0f' % execution_time + ', optimize time: %.0f' % optimize_time + ', total time: %.0f' % total_time + ' (micro s)')
	prepare, execute = calc_sum(summary)
	total = prepare + execute

	table_data = []
	table_data.append(['Operator          ', 'Prepare', 'Execution', 'Total time', 'share Prepare', 'share Execution', 'share Total'])
	for row in summary:
		row_total = row['prepare'] + row['execute']
		table_data.append([row['name'], "{:,.0f}".format(row['prepare']), "{:,.0f}".format(row['execute']), "{:,.0f}".format(row_total),
			"%.3f" % ((row['prepare']/prepare) if prepare != 0 else 0.), "%.3f" % (row['execute']/execute), "%.3f" % (row_total/total) ])
	table_data.append(['Total', "{:,.0f}".format(prepare), "{:,.0f}".format(execute), "{:,.0f}".format(total), '', '', ''])

	table = AsciiTable(table_data)
	for i in range(1, 7):
		table.justify_columns[i] = 'right'

	jit_wrapper_times = calc_sum(summary, True)

	print(table.table)
	print("")
	if experiment['experiment']['query_id'] not in d:
		d[experiment['experiment']['query_id']] = {}
	id = experiment['experiment']['engine'] if 'jit_use_jit' not in experiment['experiment'] or experiment['experiment']['jit_use_jit'] else 'interpreted'
	d[experiment['experiment']['query_id']][id] = (runtimes, (prepare, execute, total), jit_wrapper_times)

for key, query in d.iteritems():
	pipeline_runtimes, operator_runtimes, jit_wrapper_times = query['jit']
	jit_spez_time, jit_execute_time = jit_wrapper_times
	print('Query: ' + key + ', Specialization time ' + "{:,.0f}".format(jit_spez_time) + ' Jit execute time ' + "{:,.0f}".format(jit_execute_time))

	table_data = []
	table_data.append(['Time (micro s)', 'Opossum', 'Jit', 'Diff', '%', 'Diff %'])
	table = AsciiTable(table_data)
	description = [['compile pipeline', 'execution pipeline', 'optimize pipeline', 'total pipeline'], ['prepare operators', 'execute operators', 'total operators']]
	for index, value in enumerate(description):
		for _index, _value in enumerate(value):
			opossum = float(query['opossum'][index][_index])
			jit = float(query['jit'][index][_index])
			share = jit / opossum * 100 if opossum != 0 else 0
			table_data.append([_value, "{:,.0f}".format(opossum), "{:,.0f}".format(jit), "{:,.0f}".format(jit - opossum), ("%.2f" % share if opossum != 0 else 'inf') + '%', ('+' if share >= 100 else '') + ("%.2f" % (share-100) if opossum != 0 else 'inf') + '%'])

	for i in range(1, 6):
		table.justify_columns[i] = 'right'

	print(table.table)

	if str2bool(sys.argv[-1]):
		filename = key.replace(' ', '_') + '.svg'
		print("Generating diagram " + filename)
		N = 2
		opossum_total_pipeline = query['opossum'][0][3] / 1000000.
		jit_total_pipeline = query['jit'][0][3] / 1000000.
		jit_spez_time = jit_spez_time /1000000.
		jit_execute_time = jit_execute_time / 1000000.
		common_time = jit_total_pipeline - jit_spez_time - jit_execute_time
		hyrise_only = (opossum_total_pipeline, 0)
		pipeline_execition = (0, jit_total_pipeline)
		pipeline_spez = (0, jit_spez_time+common_time)
		common_operators = (common_time, common_time)
		ind = np.arange(N)    # the x locations for the groups
		width = 0.35       # the width of the bars: can also be len(x) sequence

		plt.rcParams.update({'font.size': 16})
		plt.rcParams.update({'figure.figsize': [6.5, 6]})
		p1 = plt.bar(ind, hyrise_only, color='#9D56C2')
		p2 = plt.bar(ind, pipeline_execition, color='#DC002D')
		p3 = plt.bar(ind, pipeline_spez, color='#00A739')
		p4 = plt.bar(ind, common_operators, color='#F87A12')

		plt.ylabel('Runtime (s)')
		plt.title(key.replace('TPCH', 'TPC-H '))
		plt.xticks(ind, ('Hyrise', 'Jit'))
		#plt.yticks(np.arange(0, 81, 10))
		if str2bool(sys.argv[-2]):
			plt.legend((p1[0], p2[0], p3[0], p4[0]), ('Hyrise Only Operators', 'Pipeline Execution', 'Pipeline Specialization', 'Common Operators'))

		#plt.show()
		plt.savefig(filename, transparent=True)
		plt.gcf().clear()

	print("")
