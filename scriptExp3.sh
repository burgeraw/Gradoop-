#!/bin/bash

# Experiment3(algorithmGranularity, edgeOrVertexPartitioner, withCaching, QSbatchSize, filepath, runNumber, numberOfVertices, numberOfEdges, fullyDecoupled, parallelism);

for gran in edge vertex state
do 
for fullyDec in "false" "true"
do 
	for caching in "true" "false"
	do 	
	for i in 1 2 3 4 5 
	do
		sleep 5s
		/share/hadoop/annemarie/flink-1.11.0/bin/flink run /share/hadoop/annemarie/Gradoop--1.0-SNAPSHOT.jar 3 $gran vertex $caching 1000 /share/hadoop/annemarie/resources/AL/as-skitter $i 1696415 22190596 $fullyDec 24
	done
	done
done
done
done

for fullyDec in "false" "true"
do 
	for caching in "true" "false"
	do 	
	for i in 1 2 3 4 5 
	do
		sleep 5s
		/share/hadoop/annemarie/flink-1.11.0/bin/flink run /share/hadoop/annemarie/Gradoop--1.0-SNAPSHOT.jar 3 state edge $caching 1000 /share/hadoop/annemarie/resources/EL/as-skitter.txt $i 1696415 22190596 $fullyDec 24
	done
	done
done

for QS in 10 100 1000 10000 100000 1000000 10000000 100000000 1000000000 10000000000 100000000000
do
for gran in edge vertex
do
	for i in 1 2 3 4 5 
	do
	sleep 5s	
	/share/hadoop/annemarie/flink-1.11.0/bin/flink run /share/hadoop/annemarie/Gradoop--1.0-SNAPSHOT.jar 3 $gran vertex false $QS /share/hadoop/annemarie/resources/AL/as-skitter $i 1696415 22190596 false 24

	done
done
done

#on fastest one --> test with parallelism 24 * {1, 2, 4, 8, 16}

