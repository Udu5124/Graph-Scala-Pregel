# Graph-Scala-Pregel
CSE 6331 : Map Reduce in Scala to take nodes as input and give count of number connected components of each undirected graph using pregel library functions.

  
You are asked to re-implement Project #5 (Graph Processing) using Pregel on Spark GraphX. 
That is, your program must find the connected components of any undirected graph and print the size of these connected components. 
An empty project8/src/main/scala/Graph.scala is provided, as well as scripts to build and run this code on Comet.
You should modify Graph.scala only. You should use the pregel method from the GraphX Pregel API only to write your code. 
Your main program should take the text file that contains the graph (small-graph.txt or large-graph.txt) as an argument and print the results to the output.
The stopping condition is when the number of repetition reaches 5.
