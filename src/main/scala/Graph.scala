import org.apache.spark.graphx.{Graph, VertexId, Edge, EdgeDirection}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD

object GraphComponents 
{
	def main ( args: Array[String] ) 
	{
		val conf = new SparkConf().setAppName("Connected_Components_graphx");
		val sc = new SparkContext(conf);
		//Load the file and get it to vertex and adjacent list
		val file = sc.textFile(args(0));
		val edge_rdd = file.map( line => { val (vertex, adj_list) = line.split(",").splitAt(1)
	    (vertex(0).toLong, adj_list.toList.map(_.toLong));}).flatMap(vertices => vertices._2
	    .map(vert => (vertices._1, vert)))
		.map(v => Edge(v._1, v._2, v._1))

	var initial_graph = org.apache.spark.graphx.Graph.fromEdges(edge_rdd, "defaultProperty")

	//Set every Vertex to it its VertexId
	val graph: org.apache.spark.graphx.Graph[Long, Long] = initial_graph.mapVertices((vert_id, _) => vert_id)
	
	val result = graph.pregel(Long.MaxValue, 5, EdgeDirection.Either) (
		 //Set min vertexId in adjacent list
		(id, grp, new_grp) => math.min(grp, new_grp),
		min_vertex => {
			if(min_vertex.attr < min_vertex.dstAttr){
				Iterator((min_vertex.dstId, min_vertex.attr))
			}else if(min_vertex.srcAttr < min_vertex.attr){
				Iterator((min_vertex.dstId, min_vertex.srcAttr))
			}else{
				Iterator.empty;
			}
		},
		(a,b) => math.min(a,b)
	)
	val result_i = result.vertices.map(graph => (graph._2, 1)).reduceByKey(_ + _).sortByKey(true, 0)
	val result_formatted = result_i.map { case ((k,v)) =>k+" "+v}
	result_formatted.collect().foreach(println)
	sc.stop()
	}
}
