import breeze.plot.{Figure, hist}
import breeze.stats.distributions.LogNormal
import org.apache.spark.graphx.Graph

object UpdateDistributions {


  /** Add log normal distributed weights as the property for vertices
   *
   * @param g     Your graph
   * @param mu    Expected value (Forventingsverdi)
   * @param sigma Standard deviation(Eller varians?)
   * @tparam VD VertexType
   * @tparam ED EdgeType
   * @return */
  def add_log_normal_update_distr_vertex[VD, ED](g: Graph[VD, ED], mu: Int = 100, sigma: Double = 2): Graph[Int, ED] = {
    g.mapVertices((_, _) => LogNormal(mu, sigma).draw().toInt)
  }

  /** Add log normal distributed weights as the property for vertices
   *
   * @param g     Your graph
   * @param mu    Expected value (Forventingsverdi)
   * @param sigma Standard deviation(Eller varians?)
   * @tparam VD VertexType
   * @tparam ED EdgeType
   * @return */
  def add_log_normal_update_distr_edge[VD, ED](g: Graph[VD, ED], mu: Int = 100, sigma: Double = 2): Graph[VD, Int] = {
    g.mapEdges(_ => LogNormal(mu, sigma).draw().toInt)
  }

  /** Add log normal distributed weights as the property for vertices and edges
   *
   * Idk why this isn't just one function. TODO make a general function where the distribution can be inserted
   * @param mu Expected value
   * @param sigma Standard deviation (or variance?)
   */
  def add_log_normal_update_distr_graph[VD, ED](g: Graph[VD, ED], mu: Int = 100, sigma: Double = 2): Graph[Int, Int] = {
    val g1 = add_log_normal_update_distr_vertex(g, mu, sigma)
    add_log_normal_update_distr_edge(g1, mu, sigma)
  }



  /** Plot the distribution of the Int associated with vertices
   *
   * @param g        Your graph
   * @param bins     Number of bins for the histogram
   * @param title    Title for the generated diagram
   * @param filename Filename for the diagram
   * @tparam ED Edges can have any type */
  def plotUpdateDistributionVertices[ED](g: Graph[Int, ED], bins: Int = 100, title: String = "Here your dist", filename: String = "plot.png"): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    p += hist(g.vertices.values.collect(), bins)
    p.title = title
    f.saveas(filename)
  }

  /** Plot the distribution of the Int associated with edges
   *
   * @param g        Your graph
   * @param bins     Number of bins for the histogram
   * @param title    Title for the generated diagram
   * @param filename Filename for the diagram
   * @tparam VD Vertices can have any type */
  def plotUpdateDistributionEdges[VD](g: Graph[VD, Int], bins: Int = 100, title: String = "Here your dist", filename: String = "plot.png"): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    val h = g.edges.collect().map(x => x.attr.toDouble)
    p += hist(h, bins)
    p.title = title
    f.saveas(filename)
  }
}


