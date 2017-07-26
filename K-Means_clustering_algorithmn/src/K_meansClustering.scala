import org.apache.spark.util.Vector
import java.lang.Math
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object KMeansClustering {
  
  def KMeansCluster(p:Pair[Double,Double], cent: Array[Pair[Double,Double]]): Int = {
    var bIndex = 0
    var close = Double.PositiveInfinity
    for(i <- 0 until cent.length){
      var xDist= (p._1-cent(i)._1)*(p._1-cent(i)._1)
      var yDist= (p._2-cent(i)._2)*(p._2-cent(i)._2)
      var tDist=Math.sqrt(xDist+yDist)
      if (tDist < clos) {
        clos = tDist
        bIndex = i
      }
    }
    bIndex
  }
  
  def main(args: Array[String]) {
    
var File ="TestFile/Q1_testkmean.txt"
var ssc = new SparkContext("local", "KMeansClustering App", "/path/to/spark-0.9.1-incubating",List("target/scala-2.10/simple-project_2.10-1.0.jar"))
var lData = ssc.textFile(File, 2).map(line => (line.split(" ")(0).toDouble, line.split(" ")(1).toDouble))
lData.saveAsTextFile("src/data/logData")
    
var k=3
var bool=0
var centroids = lData.takeSample(false, k, 42)

    while(bool<10)
    {
        bool=bool+1
        println("Iter number: " + bool)
        for(i <- 0 until centroids.length){
        println(centroids(i)._1 + "-->>>" + centroids(i)._2)

    }
     
      var closestCentroid = lData.map (p => (KMeansCluster(p, centroids), (p,1)))
      var pStats = closestCentroid.reduceByKey{case (((x1, y1),z1), ((x2, y2),z2)) => ((x1 + x2, y1 + y2),(z1+z2))}
      pStats.saveAsTextFile("src/data/pStats")
      
      var Points = pStats.map {pair => ((pair._2._1._1 / pair._2._2),(pair._2._1._2 / pair._2._2))}.toArray
      
      for (j <- 0 until Points.size) {
        centroids(j)= Points(j) 
      }
    }
  } 
}