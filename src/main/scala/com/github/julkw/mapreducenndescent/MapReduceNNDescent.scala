package com.github.julkw.mapreducenndescent

import java.io.{BufferedInputStream, FileInputStream}
import java.nio
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.math.{pow, sqrt}
import scala.language.postfixOps

case class Node(index: Int, location: Seq[Float])

case class Neighbor(node: Node, distance: Double, var isNew: Boolean, isReverse: Boolean)

object MapReduceNNDescent {
  val mapReduceApp = new MapReduceNNDescent()

  def main(args: Array[String]): Unit = {
    mapReduceApp.run()
  }
}

class MapReduceNNDescent {

  val path: String = "../dNSG/data/sift100k/sift100k_base.fvecs"
  // val numCores: Int = 20
  // val numPartitions: Int = 240
  // val k = 100
  val initialNeighbors = 10
  val iterations = 10

  def run(): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("MapReduce NNDescent")
      //.master("local[4]")
      //.config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.maxResultSize", "20g")
    val spark = sparkBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    // Set the default number of shuffle partitions to 5 (default is 200, which is too high for local deployment)
    // spark.conf.set("spark.sql.shuffle.partitions", s"$numPartitions")

    // read data and generate random graph
    val data = readDataFloat(path)//.slice(0, 1000)
    println("Read " + s"${data.length}" + " lines of data from " + s"$path")
    for (k <- Seq(10, 25, 50, 100)) {
      runMapReduce(spark, k, data)
    }
  }

  def runMapReduce(spark: SparkSession, k: Int, data: Array[Array[Float]]): Unit = {
    val nnd = new NNDescent(k)

    println("Run NNDescent with k: " + s"$k")

    val graph = data.indices.map { nodeIndex =>
      val node = Node(nodeIndex, data(nodeIndex).toSeq)
      val neighbors = randomNodes(initialNeighbors, data.length).toSeq.map { neighborIndex =>
        Neighbor(Node(neighborIndex, data(neighborIndex).toSeq), nnd.euclideanDist(data(nodeIndex), data(neighborIndex)), isNew = true, isReverse = false)
      }
      (node, neighbors)
    }.toList
    println("Finished building graph")

    printGraphStats(graph.toArray)
    var rdd = spark.sparkContext.parallelize(graph)
    var combinedIterationTime: Long = 0
    (0 until iterations).foreach { it =>
      val beforeIt = System.currentTimeMillis()
      rdd = nnd.localJoin(rdd)
      val updatedGraph = rdd.collect()
      val afterIt = System.currentTimeMillis()
      // printGraphStats(updatedGraph)
      val itDuration = (afterIt - beforeIt)/1000
      if (updatedGraph.exists(node => node._2.exists(neighbor => neighbor.isNew))) {
        combinedIterationTime += itDuration
        println("Something changed in iteration " + s"$it"+ ", which took " + s"$itDuration" + " seconds")
      } else {
        println("Nothing changed in the last iteration, stopping NNDescent")
        println("All " + s"$it" + " iterations together took " + s"$combinedIterationTime" + " seconds\n\n\n")
        return
      }
    }
    println("All " + s"$iterations" + " iterations together took " + s"$combinedIterationTime" + " seconds\n\n\n")
  }

  def printGraphStats(graph: Array[(Node, Seq[Neighbor])]): Unit = {
    val avgDist = averageDistance(graph)
    println("average distance in graph: " + avgDist)
    val avgDeg = graph.map(node => node._2.length).sum / graph.length
    println("average degree of graph " + avgDeg)
  }

  // Create initial graph
  def byteArrayToLittleEndianInt(bArray: Array[Byte]) : Int = {
    val bb: nio.ByteBuffer = ByteBuffer.wrap(bArray)
    bb.order(ByteOrder.LITTLE_ENDIAN)
    bb.getInt()
  }

  def byteArrayToLittleEndianFloat(bArray: Array[Byte]) : Float = {
    val bb: nio.ByteBuffer = ByteBuffer.wrap(bArray)
    bb.order(ByteOrder.LITTLE_ENDIAN)
    bb.getFloat()
  }

  def readDataFloat(filename: String): Array[Array[Float]] = {
    // read dimensions for proper grouping
    val bis = new BufferedInputStream(new FileInputStream(filename))
    bis.mark(0)
    val dimArray: Array[Byte] = Array.fill(4){0}
    bis.read(dimArray)
    val dimensions = byteArrayToLittleEndianInt(dimArray)
    bis.reset()

    val data = Iterator.continually(bis.read).takeWhile(input => input != -1).map(_.toByte)
      .grouped(4).toSeq
      .grouped(dimensions + 1).map{
      byteValues =>
        byteValues.slice(1, byteValues.length).map(value => byteArrayToLittleEndianFloat(value.toArray)).toArray
    }.toArray
    data
  }

  def randomNodes(nodesNeeded: Int, graphSize: Int): Set[Int] = {
    val r = scala.util.Random
    var nodes: Set[Int] = Set.empty
    while (nodes.size < nodesNeeded) {
      nodes += r.nextInt(graphSize)
    }
    nodes
  }

  def averageDistance(graph: Array[(Node, Seq[Neighbor])]): Double = {
    graph.map { case (node, neighbors) =>
      neighbors.map(_.distance).sum / neighbors.length
    }.sum / graph.length
  }
}

class NNDescent(k: Int) extends java.io.Serializable {
  def localJoin(graph: RDD[(Node, Seq[Neighbor])]): RDD[(Node, Seq[Neighbor])] = {
    graph.flatMap { case (node, neighbors) =>
      val reverseNeighbors = neighbors.map(neighbor => (neighbor.node, Seq(Neighbor(node, neighbor.distance, neighbor.isNew, isReverse = true))))
      val normalNeighbors = (node, neighbors)
      // collect normal and reverse neighbors for each node
      reverseNeighbors :+ normalNeighbors
    }
    .reduceByKey(_ ++ _ distinct)
    .flatMap { case (node, neighbors) =>
      // join neighbors
      val potentialNeighbors = neighbors.combinations(2)
        .filter(combination => combination(0).isNew || combination(1).isNew)
        .flatMap { pair =>
          val dist = euclideanDist(pair(0).node.location, pair(1).node.location)
          val edge1 = (pair(0).node, Seq(Neighbor(pair(1).node, dist, isNew = true, isReverse = false)))
          val edge2 = (pair(1).node, Seq(Neighbor(pair(0).node, dist, isNew = true, isReverse = false)))
          Seq(edge1, edge2)
        }.toList

      val currentNeighbors = neighbors.filterNot(_.isReverse).sortBy(_.distance)
      currentNeighbors.foreach(_.isNew = false)
      potentialNeighbors :+ (node, currentNeighbors)
    }
    .reduceByKey { (collectedNeighbors, potentialNeighbors) =>
        reducePotentialNeighbors(collectedNeighbors, potentialNeighbors)
    }
  }

  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    var sum = 0.0d
    pointX.indices.foreach { index =>
      sum += pow(pointX(index).toDouble - pointY(index).toDouble, 2)
    }
    sqrt(sum)
  }

  // TODO: change neighbors to largest is in front for faster closeEnough check?
  def reducePotentialNeighbors(neighbors: Seq[Neighbor], potentialNeighbors: Seq[Neighbor]): Seq[Neighbor] = {
    if (potentialNeighbors.length > 1) {
      mergeNeighborLists(neighbors, potentialNeighbors)
    } else {
      val potentialNeighbor = potentialNeighbors.head
      val closeEnough = neighbors.length < k || neighbors.last.distance > potentialNeighbor.distance
      if (closeEnough) {
        val alreadyExists = neighbors.exists(neighbor => neighbor.node.index == potentialNeighbor.node.index)
        if (!alreadyExists) {
          val position = neighbors.indexWhere(_.distance > potentialNeighbor.distance)
          if (position < 0) {
            (neighbors :+ potentialNeighbor).slice(0, k)
          } else {
            neighbors.patch(position, potentialNeighbors, 0).slice(0, k)
          }
        } else {
          neighbors
        }
      } else {
        neighbors
      }
    }
  }

  def mergeNeighborLists(neighbors: Seq[Neighbor], potentialNeighbors: Seq[Neighbor]): Seq[Neighbor] = {
    var alreadyChosenNeighbors: Set[Int] = Set.empty
    (neighbors ++ potentialNeighbors)
      .sortBy(n => (n.distance, n.isNew))
      .collect {
        case neighbor if !alreadyChosenNeighbors.contains(neighbor.node.index) && alreadyChosenNeighbors.size < k =>
          alreadyChosenNeighbors += neighbor.node.index
          neighbor
      }
  }

  // used in debugging
  def isSorted(n: Seq[Neighbor]): Boolean = n match {
    case Seq() => true
    case Seq(_) => true
    case _ => n.sliding(2).forall { case Seq(x, y) => x.distance <= y.distance }
  }
}
