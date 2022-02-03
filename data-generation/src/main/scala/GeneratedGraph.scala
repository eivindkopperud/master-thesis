import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{EdgeRDD, Graph, VertexRDD}

object GeneratedGraph {

  type V = VertexRDD[String]
  type E = EdgeRDD[String]
  def genGraph(sc:SparkContext):Graph[String,String] =
    GraphGenerators.logNormalGraph(sc, 5, 5)
    .mapVertices( (id, num) => "hei")
      .mapEdges(edge => "im an edge")
}
