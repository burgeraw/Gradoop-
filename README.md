# GRADOOP++ #
By Annemarie Burger

## Theses 
- [KTH Thesis of Gelly-streaming (2015)](http://www.diva-portal.org/smash/get/diva2:830662/FULLTEXT01.pdf)

## Papers ##

Aim for credible (ideally full) research papers from top venues of DB field, namely SIGMOD, VLDB, ICDE, EDBT, KDD.

Since you like "triangle counting" problem and want to test Gradoop++ with it; the goal of the thesis can be twofold:

1 -- Create a solid prototype Gradoop++ system that does graph stream processing on windows

Structure idea: Windowed edge stream (Flink) -> graph algorithm (Stateful Functions) -> aggregate results + graph sketches (Flink)

     Subgoals
     
     1.1 -- StateACC inside the prescribed window (time-based or count-based) accurately maintained. With AL, EL and CSR format.
     
     1.2 -- StateAPPROX for the whole graph stream (or a larger subset than the defined processing window) approximately maintain with graph sketches.
     
     1.3 -- StateACC and StateAPPROX should be incrementally maintained.
     
2 -- Application of focus: Triangle Counting (TC) Problem

    2.1 -- Exact vs Approximate TC
          
          We can support both with Gradoop++ if we have StateACC and StateAPPROX properly implemented.
    
    2.2 -- Centralized vs Distributed
          
          Aim for Distributed papers
          
          2.2.1 Graph State: Check how do they maintain graph state and how do they process it. 
          
          Do not focus on each TC algorithm specifics for now!
          
    2.3 -- Static vs Streaming
    
          Aim for Streaming methods. 
          
          Check:
          
              2.3.1 Streaming model: What is the streaming model assumed in each paper (e.g. edges are streamed?) 
              
              2.3.2 Window Processing: Do they maintain a window for TC inside the window or they count them on unbounded graph streams. 
              
              Do not focus on each TC algorithm specifics for now!
              
  For each paper you find/read and is really related please create a google doc that answers the above questions.

### Related to Systems/Gradoop++ ###
- [Gradoop: Technical Report (2015)](https://www.dropbox.com/s/kg49nz8z3kcfa19/GradoopTR.pdf?dl=0)
- [Scalability! But at what COST? (2015)](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-mcsherry.pdf), and  [slides](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-mcsherry.pdf). Let's keep COST in mind!
- [Analyzing Extended Property Graphs with Apache Flink (NDA 2016)](https://dbs.uni-leipzig.de/file/EPGM.pdf)
- [StreamApprox: approximate computing for stream analytics (Middleware 2017)](https://dl.acm.org/doi/abs/10.1145/3135974.3135989)
- [GraphTides: a framework for evaluating stream-based graph processing platforms (GRADES 2018)](https://dl.acm.org/doi/pdf/10.1145/3210259.3210262)
- [Practice of Streaming and Dynamic Graphs: Concepts, Models, Systems, and Parallelism(2019)](https://arxiv.org/pdf/1912.12740.pdf)

### Related to Application/Triangle Counting ###
- [Graph Stream Algorithms: A survey (2014)](https://people.cs.umass.edu/~mcgregor/papers/13-graphsurvey.pdf)
- [Graph sketches: sparsification, spanners, and subgraphs (Sigmod 2016)](https://dl.acm.org/doi/pdf/10.1145/2213556.2213560)

## Presentations ##
- [Approximate Query Library on Flink (Vasiloudis 2018)](https://www.dropbox.com/s/vd4xhamcnbvwfou/Flink%20Seattle%20Meetup.pdf?dl=0) with [code](https://github.com/tlindener/ApproximateQueries/) available
- [Single-pass Graph Streaming Analytics with Gelly Streaming (Vasia and Paris 2016)](https://www.dropbox.com/s/9ug1s0emf9aozg8/single-pass-graph-stream-analytics-gelly-streaming.pdf?dl=0)
- [Spark Streaming and GraphX](http://ictlabs-summer-school.sics.se/2016/slides/spark_streaming_graphx.pdf)
- [Graph Stream Processing (Paris)](https://www.dropbox.com/s/u2eb52q1e8nvpur/paris-graph-stream-processing.pdf?dl=0)
- [Flink forward presentation: Deep dive into Stateful Functions](https://www.youtube.com/watch?v=tuSylBadNSo&list=PLDX4T_cnKjD0ngnBSU-bYGfgVv17MiwA7&index=22)

## Experiments ##
- We can use [GraphTides](https://graphtides.github.io/) framework (by Pietzuch) to plug in Gradoop++ and GraphX for proper comparison

## Datasets ##
- [Network Repository. An Interactive Scientific Network Data Repository.](http://networkrepository.com/index.php)
- [Streaming Graphs Datasets](https://www.eecs.wsu.edu/~yyao/StreamingGraphs.html)
- [Graphs Datasets](https://sites.google.com/site/xiaomengsite/research/resources/graph-dataset)
- [Yahoo Graph Datasets](https://webscope.sandbox.yahoo.com/catalog.php?datatype=g&guccounter=1)

## Resources ##
- Foundation of operators and their implementation from [Gelly-streaming](https://github.com/vasia/gelly-streaming)
- [GRADOOP wiki and code](https://github.com/dbs-leipzig/gradoop/wiki)
- [Approximate Query Library on Flink](https://github.com/tlindener/ApproximateQueries/)
