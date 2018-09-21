#!/usr/bin/env python

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import NullFormatter  # useful for `logit` scale

def str2bool(v):
	return v.lower() in ("yes", "true", "t", "1")

def format_diff(diff, swap = False):
	if diff >= 0:
		return colored('+' + "{0:.0%}".format(diff), 'green' if swap == False else 'red')
	else:
		return colored("{0:.0%}".format(diff), 'red' if swap == False else 'green')

def enabled(bool):
	return 'enabled' if bool else 'disabled'

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
	opossum = json.load(old_file)

with open(sys.argv[2]) as new_file:
	jit = json.load(new_file)

has_interpret = len(sys.argv) > 3 and not str2bool(sys.argv[3])
interpret = jit
if has_interpret:
	with open(sys.argv[3]) as file:
		interpret = json.load(file)
	if (str2bool(opossum['context']['jit']) or not str2bool(jit['context']['jit']) or str2bool(jit['context']['interpret']) or not str2bool(interpret['context']['jit']) or not str2bool(interpret['context']['interpret'])):
		print("Usage: " + sys.argv[0] + " opossum.json jit.json interpret.json")
		exit()
else:
	if str2bool(opossum['context']['jit']) or not str2bool(jit['context']['jit']) or ('interpret' in jit['context'] and str2bool(jit['context']['interpret'])):
		print("Usage: " + sys.argv[0] + " opossum.json jit.json interpret.json")
		exit()

current = 0

names = []
log = True
spez_time=True
if log:
	n_col = 4
else:
	n_col = 4 if has_interpret else 3
tmp = zip(opossum['benchmarks'], jit['benchmarks'], interpret['benchmarks'])
if not log:
	tmp = tmp[0:1] + tmp[2:-3] #  tmp[:-3]
N = n_col * len(tmp)
for old, new, interpret in tmp:

	print('Adding query ' + old['name'] + ' to graph')
	id = old['name'].replace('TPC-H ', '')
	names.append(id)

	jit_prepare, jit_execute, jit_total = calc_sum(new['operators'], True)
	interpret_prepare, interpret_execute, interpret_total = calc_sum(interpret['operators'], True)
	opossum_total_pipeline = int(old['avg_real_time_per_iteration']) / 1000000000.
	jit_total_pipeline = int(new['avg_real_time_per_iteration']) / 1000000000.
	interpreted_total_pipeline = int(interpret['avg_real_time_per_iteration']) / 1000000000.

	common_time = jit_total_pipeline - jit_total / 1000000.
	hyrise_only = (opossum_total_pipeline, 0, 0) if has_interpret else (opossum_total_pipeline, 0)
	pipeline_spez = (0, interpreted_total_pipeline, jit_total_pipeline) if has_interpret else (0, jit_total_pipeline)
	pipeline_execition = (0, 0, jit_execute / 1000000. + common_time) if has_interpret else (0, jit_execute / 1000000. + common_time)
	common_operators = (common_time, common_time, common_time) if has_interpret else (common_time, common_time)
	if log:
		hyrise_only = (opossum_total_pipeline, 0, 0)
		pipeline_execition = (0, jit_total_pipeline, 0)
		pipeline_spez = (0, 0, jit_total_pipeline - jit_prepare / 1000000.)
		ind = np.arange(current, current + 3)
	else:
		ind = np.arange(current, current + (3 if has_interpret else 2))    # the x locations for the groups
	width = 0.35       # the width of the bars: can also be len(x) sequence

	plt.rcParams.update({'font.size': 16})
	plt.rcParams.update({'figure.figsize': [12, 6]})
	p1 = plt.bar(ind, hyrise_only, color='#9D56C2')
	if spez_time:
		p2 = plt.bar(ind, pipeline_spez, color='#00A739')
	p3 = plt.bar(ind, pipeline_execition, color='#DC002D')
	if not log:
		p4 = plt.bar(ind, common_operators, color='#F87A12')
	current += n_col


plt.ylabel('Runtime (log) (s)' if log else 'Runtime (s)')
plt.title('TPC-H Runtimes, scale ' + str(opossum['context']['scale_factor']) +', Encoding '+ opossum['context']['encoding']['default']['encoding'] + ', MVCC ' + enabled(opossum['context']['using_mvcc']) + ', Pella')
offset = 1 if log else 0.5
plt.xticks(np.arange(offset, N + offset, n_col), names)
if log:
	plt.yscale('log')
#plt.yticks(np.arange(0, 81, 10))
if str2bool(sys.argv[-1]):
	if log:
		plt.legend((p1[0], p2[0], p3[0]), ('Hyrise', 'Specialized w/o time of specialization', 'Specialized'))
	else:
		if spez_time:
			plt.legend((p1[0], p2[0], p3[0], p4[0]), ('Hyrise', 'Pipeline Specialization', 'Specialized', 'Common Operators'))
		else:
			plt.legend((p1[0], p3[0], p4[0]), ('Hyrise', 'Specialized', 'Common Operators'))

#plt.show()
ax = plt.axes()
#ax.set_yticks(0.1, ax.get_yticks()) #plt.axis().yaxis.YTick.update_position(0.1) #set_minor_formatter(NullFormatter())
ax.yaxis.grid()
plt.ylim(0 if log else 0, 6840 if log else 3)
name = 'tpch_runtimes_s_' + str(opossum['context']['scale_factor']) + '_encod_' + opossum['context']['encoding']['default']['encoding'] + '_mvcc_' + enabled(opossum['context']['using_mvcc']) + '_log_' + str(log)
plt.savefig(name + '.svg', transparent=True)
plt.gcf().clear()
