#!/bin/bash

./bin/start-cluster.sh

# Experiment 1a. Usage: filepath, numberOfEdges, runNumber, datastructure, edgeOrVertexPartitioner, [numberOfVertices]

for i in 1 2 3 4 5
do
    for datastructure in AL EL sortedEL
    do
	./bin/flink Gradoop___jar/Gradoop++.jar 1a /resources/as-skitter.txt 11095298 i datastructure edge
	./bin/flink Gradoop___jar/Gradoop++.jar 1a /resources/as-skitter.txt 11095298 i datastructure vertex 1696415
	./bin/flink Gradoop___jar/Gradoop++.jar 1a /resources/com-orkut.txt 117185083 i datastructure edge
	./bin/flink Gradoop___jar/Gradoop++.jar 1a /resources/com-orkut.txt 117185083 i datastructure vertex 3072441
	#etc.
    done
done
	    

# Experiment 1b. Usage: parallelism, runNumber, filepath, datastructure, edgeOrVertexPartitioner, numberOfEdges, [numberOfVertices]

for i in 1 2 3 4 5
do
    for datastructure in AL EL sortedEL
    do
	for parallelism in 25 50 100 200
	do
	    ./bin/flink Gradoop___jar/Gradoop++.jar 1b parallelism i /resources/as-skitter.txt datastructure edge 11095298
	    ./bin/flink Gradoop___jar/Gradoop++.jar 1b parallelism i /resources/as-skitter.txt datastructure vertex 11095298 1696415
	done
    done
done 

./bin/stop-cluster.sh