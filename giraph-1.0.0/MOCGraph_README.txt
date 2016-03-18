---------------------------------------------------------
Introduction:
---------------------------------------------------------
MOCgraph is an experimental distributed graph processing 
framework that built on top of Apache Giraph.

MOCgraph is designed to reduce the memory footprint and 
improve the scalability for distributed graph processing.
Our paper is published at VLDB Vol8, No.4, December 2014,
http://www.vldb.org/pvldb/vol8/p377-zhou.pdf

---------------------------------------------------------
Environment Setup:
---------------------------------------------------------
As MOCgraph is built on top of Apache Giraph v1.0.0, you 
need to set up a Giraph Cluster as described in GIRAPH-README 
file. Theoretically, MOCgraph can run on all hadoop versions 
that Giraph supports, however, we have only tested MOCgraph
with hadoop-1.0.0, hadoop-0.20.203, hadoop-0.20.2.

For example, if you have a hadoop-0.20.203.0 cluster, you
can,

cd giraph-1.0.0/giraph-core
mvn -Phadoop-0.20.203.0 compile

then you can use giraph-core/target/giraph-1.0.0-for-hadoop-
0.20.203-jar-with-dependencies.jar as mocgraph.jar

---------------------------------------------------------
Running Example:
---------------------------------------------------------
A typical job submission command looks like this:
"hadoop jar mocgraph.jar edu.pku.db.mocgraph.example.PageRank
-Dmapred.child.java.opts=-Xmx1024M -indir testGraph -outdir 
prout -w 10 -v"
We extract several algorithms from our main trunk into this 
branch, namely:
-PageRank
-ConnectedComponent
-Landmark
-TriangleCounting

Please type -h to see the detailed usage of parameters for 
each job.

Commonly, there are several available options to specify, 
-indir input path or directory (on hdfs)
-outdir output directory (on hdfs)
-w worker number
-ooc use out-of-core mode
under ooc, one can further specify the following,
     -pn 10
     	 assign 10 partitions to each worker process
	 -nimp 5 
	     there're 5 partitions that allows to be lay
	     in memory. 
     -imp 0.3
	     there're 30% memory for buffering out-of-core
		 messages.
	(we plan to dynamically set the number according to 
	 the available memory in Runtime, but for now, it's 
	 better be specified if you use out-of-core engine)
     -es 
	     use edge separation
	(not all algorithms can use this, please refer to our
	 paper to see its constraints)
-rg Run in Raw Giraph

---------------------------------------------------------
GraphInputFormat:
---------------------------------------------------------
a)For algorithms that do not need edge weights, like PageRank,
ConnectedComponent, TriangleCounting, the graph input 
format is a text file with each line describing the adjacency 
list of a vertex:

"vertex_id	neighbor_id1	neighbor_id2	neighbor_id3"

which is separated by '\t'.

b)For algorithms with edge weights, like Landmark, each line
looks like this:

"vertex_id	neighbor_id1	weight1	neighbor_id2	weight2"

which is also separated by '\t'.

---------------------------------------------------------
Write a MOCgraph program:
---------------------------------------------------------
User needs to write a vertex class that extends Giraph Vertex
just as a normal Giraph Job. Except that, you need to override 
$computeSingleMsg$ method and $sendMessages$ method for MOCgraph, 
instead of $compute()$ method for Giraph.

The interface is listed as follows,

"computeSingleMsg(V value, M message, long superstep)
sendMessages(V value, long superstep)"

You need also override "createNewValue(V old)" method to 
run withsynchronous engine.

You need to call $wakeUp$ method explicitly to 
For synchronous execution, PLEASE DO NOT use getValue(), 
setValue(), getSuperstep() in these two methods, which 
are provided by original Giraph. We are meant to obsolete 
these methods from MOCgraph, however, it's not easy if 
we need to support Giraph vertex program simultaneously. 
So the current way to deal with this is to NOT USE them. 

Users can only manipulate the value we supplied, and can
only see the logical superstep number as a parameter. 

---------------------------------------------------------
Out-of-core tuning
---------------------------------------------------------
Currently, out-of-core parameters are manually set just as 
Giraph does. 
In our experiments, we tune these parameters carefully to 
achieve a relatively good performance.

