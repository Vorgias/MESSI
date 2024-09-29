#!/bin/bash
declare -A datasets
num_rows=2
# num_rows=1
num_columns=2

datasets[1,1]="/home1/public/geopat/datasets/dataset1GB.bin"
datasets[1,2]="1048576"

#datasets[2,1]="/home/ekosmas/datasets/dataset10GB.bin"
#datasets[2,2]="10485760"


for ((i=1;i<=num_rows;i++)) 
do
	filename="results/results_[${datasets[$i,2]}].txt"

	echo "Dataset [${datasets[$i,1]}] with size [${datasets[$i,2]}]"
	echo "Dataset [${datasets[$i,2]}] JEMALLOC" >> $filename


	for version in 9990
	do
		echo "Running version [$version]"
		echo "Running version [$version]" >> $filename

		for iteration in 1 # number of runs
		# for iteration in 1 2 # number of runs
		do
	        LD_PRELOAD=`jemalloc-config --libdir`/libjemalloc.so.`jemalloc-config --revision` ./bin/ads --dataset ${datasets[$i,1]} --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size ${datasets[$i,2]} --flush-limit 1000000 --cpu-type 80 --function-type $version --in-memory
	        # ./bin/ads --dataset ${datasets[$i,1]} --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size ${datasets[$i,2]} --flush-limit 1000000 --cpu-type 80 --function-type $version --in-memory
	    done
	done

done


