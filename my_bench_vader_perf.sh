#!/bin/bash
declare -A datasets

num_columns=6

num_rows=1
datasets[1,1]="/home1/public/vorgias/dataset/1Gdataset.bin "
datasets[1,2]="1048576"
datasets[1,3]="/home1/public/vorgias/dataset/test2size100queries"
datasets[1,4]="100"
# datasets[1,5]="/home1/public/vorgias/dataset/labelexample.bin" 
datasets[1,5]="/home1/public/vorgias/dataset/labelexample3attr.bin" 
# datasets[1,6]="/home1/public/vorgias/dataset/Querylabelexample.bin"
datasets[1,6]="/home1/public/vorgias/dataset/100GdatasetLQueryLabelsAll2.bin"
iterations=1
minattrvalue=0
maxattrvalue=10
for ((i=1;i<=num_rows;i++)) 
do
	filename="results/results_[${datasets[$i,2]}].txt"

	echo "Dataset [${datasets[$i,1]}] with size [${datasets[$i,2]}]"
	echo "Dataset [${datasets[$i,2]}]" >> $filename

	for num_threads in 40
	do
		echo "Threads [$num_threads]"
		echo "Threads [$num_threads]" >> $filename

		for read_block_length in 20000 
		do

			echo "Read Block Length [$read_block_length]"
			echo "Read Block Length [$read_block_length]" >> $filename

			for ts_group_length in 64 #128 256 512 1024 2048 4096 16384 
			do
				echo "ts-group-length [$ts_group_length]"
				echo "ts-group-length [$ts_group_length]" >> $filename
				for version in 9990 			
				do
					echo "Running version [$version]"
					echo "Running version [$version]" >> $filename

					backoff_power=-1
					echo "Backoff set to [$backoff_power]"
					echo "Backoff set to [$backoff_power]" >> $filename

					for iteration in 1 #number of runs
					do
		        		LD_PRELOAD=/lib/x86_64-linux-gnu/libjemalloc.so.2  ./bin/ads --dataset ${datasets[$i,1]} --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size ${datasets[$i,2]} --flush-limit 1000000 --cpu-type 80 --function-type $version --in-memory --ts-group-length $ts_group_length --backoff-power $backoff_power --queries ${datasets[$i,3]} --queries-size ${datasets[$i,4]}  --cpu-type $num_threads --read-block $read_block_length --chunk-size $ts_group_length --attributes ${datasets[$i,5]} --query-attributes ${datasets[$i,6]} --query-attrmaxval $maxattrvalue --query-attrminval $minattrvalue
				    done
				done

			done
		done
	done
done

iterations=1



