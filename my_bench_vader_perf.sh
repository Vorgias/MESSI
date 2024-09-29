#!/bin/bash
declare -A datasets
num_columns=4

num_rows=1
datasets[1,1]="/spare/ekosmas/Datasets/Random/dataset100GB.bin"
datasets[1,2]="104857600"
datasets[1,3]="/spare/ekosmas/Datasets/Random/query100.bin "
datasets[1,4]="100"



iterations=1

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

				# For FAI , DO ALL OPT , DO ALL, CAS
				for version in 99985 #99981000 99982000 999820 #9998200 999820  				
				do
					echo "Running version [$version]"
					echo "Running version [$version]" >> $filename

					backoff_power=-1
					echo "Backoff set to [$backoff_power]"
					echo "Backoff set to [$backoff_power]" >> $filename

					for iteration in 1 2 3 4 5 #6 7 8 9 10  #number of runs
					do
		        		LD_PRELOAD=`jemalloc-config --libdir`/libjemalloc.so.`jemalloc-config --revision` perf stat -a -e cache-references,cache-misses,L1-dcache-load,L1-dcache-loads-misses,L1-dcache-stores,cache-references:u,cache-misses:u,L1-dcache-load:u,L1-dcache-loads-misses:u,L1-dcache-stores:u -r $iterations -o ./perf/perf[${datasets[$i,2]}.$num_threads.$version.$read_block_length.$ts_group_length.$backoff_power].out ./bin/ads --dataset ${datasets[$i,1]} --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size ${datasets[$i,2]} --flush-limit 1000000 --cpu-type 80 --function-type $version --in-memory --ts-group-length $ts_group_length --backoff-power $backoff_power --queries ${datasets[$i,3]} --queries-size ${datasets[$i,4]}  --cpu-type $num_threads --read-block $read_block_length --chunk-size $ts_group_length
				    done
				done

			done

			for ts_group_length in 1310720 #128 256 512 1024 2048 4096 16384 
			do
				echo "ts-group-length [$ts_group_length]"
				echo "ts-group-length [$ts_group_length]" >> $filename

				# For FAI , DO ALL OPT , DO ALL, CAS
				for version in 99986 #99982 99982000 9998200 999777010  				
				do
					echo "Running version [$version]"
					echo "Running version [$version]" >> $filename

					backoff_power=-1
					echo "Backoff set to [$backoff_power]"
					echo "Backoff set to [$backoff_power]" >> $filename

					for iteration in 1 2 3 4 5 #6 7 8 9 10  #number of runs
					do
		        		LD_PRELOAD=`jemalloc-config --libdir`/libjemalloc.so.`jemalloc-config --revision` perf stat -a -e cache-references,cache-misses,L1-dcache-load,L1-dcache-loads-misses,L1-dcache-stores,cache-references:u,cache-misses:u,L1-dcache-load:u,L1-dcache-loads-misses:u,L1-dcache-stores:u -r $iterations -o ./perf/perf[${datasets[$i,2]}.$num_threads.$version.$read_block_length.$ts_group_length.$backoff_power].out ./bin/ads --dataset ${datasets[$i,1]} --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size ${datasets[$i,2]} --flush-limit 1000000 --cpu-type 80 --function-type $version --in-memory --ts-group-length $ts_group_length --backoff-power $backoff_power --queries ${datasets[$i,3]} --queries-size ${datasets[$i,4]}  --cpu-type $num_threads --read-block $read_block_length --chunk-size $ts_group_length
				    done
				done

			done
		done
	done
done

iterations=1



