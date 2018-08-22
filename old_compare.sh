if [ "$#" -lt 1 ]; then
    echo "Provide dir for results"
else
	dir=$1
	if [ ! -d $dir ]; then
  		echo "Directory $dir does not exist"
		exit
	fi
	if [ "${dir: -1}" != "/" ]; then
		dir=$dir"/"
	fi
	if [ "$#" -eq 1 ]; then
		cat ${dir}*.txt
		echo no jit vs jit without mvcc
	    python scripts/old_compare_benchmarks.py ${dir}result_wo_jit_wo_mvcc.json ${dir}result_w_jit_wo_mvcc.json
		echo no jit vs jit with mvcc
	    python scripts/old_compare_benchmarks.py ${dir}result_wo_jit_w_mvcc.json ${dir}result_w_ll_w_jv.json
		echo no jit vs jit with mvcc but not jit validate
	    python scripts/old_compare_benchmarks.py ${dir}result_wo_jit_w_mvcc.json ${dir}result_w_ll_wo_jv.json
		echo lazy load
		python scripts/old_compare_benchmarks.py ${dir}result_wo_ll_wo_jv.json ${dir}result_w_ll_wo_jv.json
		echo jit validate
		python scripts/old_compare_benchmarks.py ${dir}result_wo_ll_wo_jv.json ${dir}result_wo_ll_w_jv.json
		echo lazy load with jit validate enabled
		python scripts/old_compare_benchmarks.py ${dir}result_wo_ll_w_jv.json ${dir}result_w_ll_w_jv.json
		echo jit validate with lazy load enabled
		python scripts/old_compare_benchmarks.py ${dir}result_w_ll_wo_jv.json ${dir}result_w_ll_w_jv.json
		echo lazy load and jit validate
		python scripts/old_compare_benchmarks.py ${dir}result_wo_ll_wo_jv.json ${dir}result_w_ll_w_jv.json
	else
		r_dir=$2
		if [ ! -d $r_dir ]; then
	  		echo "Right directory $dir does not exist"
			exit
		fi
		if [ "${r_dir: -1}" != "/" ]; then
			r_dir=$r_dir"/"
		fi
		echo "prev:"
		cat ${dir}*.txt
		echo "next:"
		cat ${r_dir}*.txt
		declare -a files=("result_wo_jit_w_mvcc.json" "result_wo_jit_wo_mvcc.json" "result_wo_ll_wo_jv.json" "result_w_ll_wo_jv.json" "result_wo_ll_w_jv.json" "result_w_ll_w_jv.json" "result_w_jit_wo_mvcc.json")
		for i in "${files[@]}"
		do
			echo $i
			python scripts/old_compare_benchmarks.py ${dir}${i} ${r_dir}${i}
		done
	fi
fi

