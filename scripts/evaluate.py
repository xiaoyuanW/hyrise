#!/usr/bin/env python

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array

def str2bool(v):
	return v.lower() in ("yes", "true", "t", "1")

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

def calc_sum(operators):
	prepare = 0
	execute = 0
	for operator in operators:
		if operator['name'].startswith('_'):
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
	print('Query: ' + experiment['experiment']['query_id'] + ', Engine: ' + experiment['experiment']['engine'])
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

	print(table.table)
	print("")
	if experiment['experiment']['query_id'] not in d:
		d[experiment['experiment']['query_id']] = {}
	d[experiment['experiment']['query_id']][experiment['experiment']['engine']] = (runtimes, (prepare, execute, total))

for key, query in d.iteritems():
	print('Query: ' + key)
	pipeline_runtimes, operator_runtimes = query
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
	print("")
