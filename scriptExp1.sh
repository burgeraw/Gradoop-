#!/bin/bash


# Experiment 1. Usage: String filepath = args[1];String numberOfEdges = args[2];String runNumber = args[3];String datastructure = args[4};String edgeOrVertexPartitioner = args[5];String numberOfVertices = args[6];String parallelism = args[7];String timeBetweenElements = args[8];

for i in 1 2 3 4 5
do
    ./bin/flink run ./Gradoop--1.0-SNAPSHOT.jar 1 /share/hadoop/annemarie/resources/EL/as-skitter.txt 11095298 $i AL edge 1696415 24 0
    ./bin/flink run ./Gradoop--1.0-SNAPSHOT.jar 1 /share/hadoop/annemarie/resources/EL/com-friendster.ungraph.txt 1806067135 $i AL edge 65608366 24 0
    ./bin/flink run ./Gradoop--1.0-SNAPSHOT.jar 1 /share/hadoop/annemarie/resources/EL/com-orkut.ungraph.txt 117185083 $i AL edge 3072441 24 0
    ./bin/flink run ./Gradoop--1.0-SNAPSHOT.jar 1 /share/hadoop/annemarie/resources/EL/soc-LiveJournal1.txt 68993773 $i AL edge 4847571 24 0
    ./bin/flink run ./Gradoop--1.0-SNAPSHOT.jar 1 /share/hadoop/annemarie/resources/EL/Skew-5 419430400 $i AL edge 4194304 24 0

done
	    

