package com.example.bigdata

import java.nio.charset.CodingErrorAction

import org.apache.flink.graph.scala.Graph
import net.liftweb.json.{DefaultFormats, _}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.graph.{Vertex}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.graph.spargel.{GatherFunction, MessageIterator, ScatterFunction}

import scala.io.{Codec}

case class Person (
  name: String,
  id: Int,
)

case class Movie (
  id: Int,
  title: String
)

// name - nazwa bohatera, to_id - id filmu, from_id - id aktora
case class ActsIn (
                    to_id: Int,
                    from_id: Int
                  )


object Oracle extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  implicit val formats = DefaultFormats
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  // Wczytywanie ludzi
  val people = env.readTextFile("./cineasts/persons.json")
    .map(person =>
        parse(person).extract[Person]
      )

  // Wczytanie informacji o filmach
  val movies = env.readTextFile("./cineasts/movies.json")
    .map(movie =>
      parse(movie).extract[Movie]
    )

  // Wczytanie informacji o tym kto gdzie grał
  val acts = env.readTextFile("./cineasts/acts_in.json")
    .map(act =>
      parse(act).extract[ActsIn]
    )

  // Wczytanie informacji o reżyserach do tej samej klasy co powyżej (te same pola)
  val directed = env.readTextFile("./cineasts/directed.json")
    .map(act =>
      parse(act).extract[ActsIn]
    )

  // Mapowanie aktorów na wierzchołki id_aktora, obiekt Aktor
  val vertices = people.map(person => (person.id, person))

  // Połączenie danych o reżyserii i rolach, potem połączenie z aktorami po id
  val personInMovie = directed.union(acts).join(people).where(_.from_id).equalTo(_.id)
    .join(movies) // Połączenie z filmami po id
    .where(_._1.to_id)
    .equalTo(_.id)
    .map(x => (x._1._2, x._2)) // Zmapowanie na DataSet[(Person, Movie)]

  // Full join na DS po id filmu
  val edges = personInMovie.join(personInMovie).where(_._2.id).equalTo(_._2.id)
    .map(z => (z._1._1, z._2._1, z._1._2)) // mapowanie do trójek (Person, Person, Movie)
    .filter(triple => triple._1.id != triple._2.id) // odrzuć trójki z tą samą osobą
    .groupBy(triple => (triple._1, triple._2)) // grupuj po tych samych osobach
    .reduceGroup(i => {
      val first = i.next()
      (first._1.id, first._2.id, 1)
    })

  // Zbuduj graf
  var graph = Graph.fromTupleDataSet(vertices, edges, env)

  // Lista Id osób, które z nikim nie grały
  val unconnectedVerticesId = graph
    .getDegrees
    .filter(_._2.getValue == 0)
    .map(x => x._1)
    .collect()

  // Lista osób (wierzchołków), które z nikim nie grały
  val unconnectedVertices = graph.getVertices
    .filter(x => unconnectedVerticesId.contains(x.getId))
    .collect()
    .toList

  // Usuwanie wierzchołków
  graph = graph.removeVertices(unconnectedVertices)

  // Zad 1
  // Przygotuj graf do scatter-gather - id to samo, jako wartość wierzchołka jego id
  val preparedGraph = graph.mapVertices(vertex => vertex.getId)

  final class CompleteMessenger extends ScatterFunction[Int, Int, Int, Int] {
    override def sendMessages(vertex: Vertex[Int, Int]) = {
      val edges = getEdges.iterator
      while (edges.hasNext) {
        val edge = edges.next
        sendMessageTo(edge.getTarget, Math.min(vertex.getId, vertex.getValue))
      }
    }
  }

  final class VertexCompleteUpdater extends GatherFunction[Int, Int, Int] {
    override def updateVertex(vertex: Vertex[Int, Int], inMessages: MessageIterator[Int]) = {
      var minValue = vertex.getValue
      while (inMessages.hasNext) {
        val msg = inMessages.next
        if (msg < minValue) {
          minValue = msg
        }
      }
      if (vertex.getValue > minValue) {
        setNewVertexValue(minValue)
      }
    }
  }

  // Wykonaj scatter-gather
  val completeGraph = preparedGraph.runScatterGatherIteration(new CompleteMessenger, new VertexCompleteUpdater, 10)

  val completeAgg = completeGraph.getVertices
    .map(tuple => (tuple.getValue, 1))
    .groupBy(0)
    .aggregate(Aggregations.SUM, 1)

  // Policz liczbę grafów
  val graphsNumber = completeAgg.count()
  println("Liczba grafów", graphsNumber)

  // Zad 2
  if (graphsNumber > 1) {
    // id z najmniejszego grafu
    val start = completeAgg.minBy(1)
      .collect()
      .toArray
      .take(1)(0)._1

    // Pobranie id wierzchołków w podgrafie
    val smallest = completeGraph.getVertices
      .filter(_.getValue == start)
      .map(_.getId)
      .collect()
      .toSet

    // Wypisanie wyalienowanych osób
    println("Najmniejszy graf zawiera")
    val smallVertices = graph.getVertices
      .filter(vertex => smallest.contains(vertex.getId))
      .collect()

    smallVertices.foreach(vertex => println(vertex.getValue.name))
  }

  // Zad 3
  val histogram = graph.getDegrees()
    .map(stat => (stat._2, 1))
    .groupBy(0)
    .aggregate(Aggregations.SUM, 1)
    .collect()
    .toArray
    .sortBy(_._1)

  // Wypisz stopień_wierzchołka, liczbę_wierzchołków o tym stopniu
  histogram.foreach(degree =>
    println(degree._1, degree._2)
  )

  // ZAD 5
  // Sprawdź czy odległość faktycznie wynosi 3
  final class MinDistanceMessenger extends ScatterFunction[Int, Double, Double, Double] {
    override def sendMessages(vertex: Vertex[Int, Double]) = {
      val edges = getEdges.iterator
      while (edges.hasNext) {
        val edge = edges.next
        sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
      }
    }
  }

  final class VertexDistanceUpdater extends GatherFunction[Int, Double, Double] {
    override def updateVertex(vertex: Vertex[Int, Double], inMessages: MessageIterator[Double]) = {
      var minDistance = Double.MaxValue
      while (inMessages.hasNext) {
        val msg = inMessages.next
        if (msg < minDistance) {
          minDistance = msg
        }
      }
      if (vertex.getValue > minDistance) {
        setNewVertexValue(minDistance)
      }
    }
  }

  // Pozyskaj id Kevina Bacona
  val baconId = graph.getVertices
    .filter(_.getValue.name.equals("Kevin Bacon"))
    .map(_.getId)
    .collect()
    .toArray
    .take(1)(0)

  val baconGraph = graph.mapEdges(e => 1.0)
    .mapVertices(v => if (v.getId == baconId) 0.0 else Double.MaxValue)

  val result = baconGraph.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater, 10)
    .getVertices
    .map(vertex => vertex.getValue)
    .filter(_ != Double.MaxValue)

  println("Bacon distance", result.reduce(_ + _).collect().toArray.take(1)(0) / result.count())

}
