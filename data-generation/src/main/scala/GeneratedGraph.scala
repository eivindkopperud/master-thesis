import com.github.javafaker.Faker
import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{EdgeRDD, Graph, VertexRDD}

object GeneratedGraph {

  type V = VertexRDD[String]
  type E = EdgeRDD[String]

  def genGridGraph[A, B](sc: SparkContext): Graph[(Int, Int), Double] =
    GraphGenerators.gridGraph(sc, 5, 5)

  def genLogNormalGraph(sc: SparkContext, nvert: Int = 5, nedge: Int = 5): Graph[String, String] =
    GraphGenerators.logNormalGraph(sc, nvert, nedge)
      .mapVertices((id, num) => getRandomName)
      .mapEdges(edge => getRandomRelation)

  def genRMATGraph(sc: SparkContext): Graph[Int, Int] =
    GraphGenerators.rmatGraph(sc, 5, 10)

  def getRandomName: String = new Faker().leagueOfLegends().champion()

  def getRandomRelation: String = "SOMETHING"
}
