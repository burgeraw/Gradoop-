#!/bin/bash

# use 	ps -p TaskManagerPID -o rss 	in workers nodes to measure memory used, repeat exp from 3. 

for QS in 1000 1000000 1000000000
do
for gran in edge vertex state
do 
	for caching in "true" "false"
	do 	
	for i in 1 2 3 4 5 
	do
		sleep 5s
		/share/hadoop/annemarie/flink-1.11.0/bin/flink run /share/hadoop/annemarie/Gradoop--1.0-SNAPSHOT.jar 3 $gran vertex $caching $QS /share/hadoop/annemarie/resources/AL/as-skitter $i 1696415 22190596 false 24
	done
	done
done
done
