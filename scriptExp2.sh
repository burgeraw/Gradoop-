#!/bin/bash

# Experiment2(String batchSizePerPU,String runNumber,String filepath,String datastructure,String activeOrLazyPurging,String parallelism,String windowSize, String slideSize)

for cleaning in active lazy
do 
	for batchSize in 1 10 100 1000 10000 100000 1000000
	do
	for dataSt in AL EL sortedEL
	do
		for i in 1 # 2 3 4 5 
		do
			sleep 5s
			/share/hadoop/annemarie/flink-1.11.0/bin/flink run /share/hadoop/annemarie/Gradoop--1.0-SNAPSHOT.jar 2 $batchSize $i /share/hadoop/annemarie/resources/EL/com-orkut.ungraph.txt $dataSt $cleaning 24 3000 1000
		done

	done
	done
done
