#!/bin/bash

# Experiment5(withCaching, QSbatchSize, filepath, runNumber, numberOfVertices, numberOfEdges, fullyDecoupled, parallelism, timeToRun, repeats, windowSize, slideSize, withQS);


#To prove est works properly on max window
for p in 1 2 4 8 16 24
do
	for QS in "false" "true"
	do
	/share/hadoop/annemarie/flink-1.11.0/bin/flink run /share/hadoop/annemarie/Gradoop--1.0-SNAPSHOT.jar 5 false 1000 /share/hadoop/annemarie/resources/AL/as-skitter 1 1696415 22190596 false $p 100 1000 1000000 null $QS
	sleep 5s
	done
done
