package com.example.bigdata

import java.nio.charset.CodingErrorAction
import net.liftweb.json.{DefaultFormats, _}

import scala.collection.mutable.ListBuffer
import scala.io.{Codec, Source}

case class Person (
  name: String,
  id: Int,
  labels: List[String]
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

// klasa do przechowywania trójek (osoba1, osoba2, film)
case class Connection (
                        actor1: Int,
                        actor2: Int ,
                        movie: Int
                      )

object Oracle extends App {
  //  val env = ExecutionEnvironment.getExecutionEnvironment
  implicit val formats = DefaultFormats
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  // Wczytywanie ludzi
  val peopleFile = "./cineasts/persons.json"
  val peopleJSONString = Source.fromFile(peopleFile)
  val people = peopleJSONString
    .getLines()
    .map(person =>
        parse(person).extract[Person].id -> parse(person).extract[Person]
      )
    .toMap
  peopleJSONString.close()

  // Wczytanie informacji o filmach
  val moviesFile = "./cineasts/movies.json"
  val moviesJSONString = Source.fromFile(moviesFile)
  val movies = moviesJSONString
    .getLines()
    .map(movie =>
      parse(movie).extract[Movie].id -> parse(movie).extract[Movie]
    )
    .toMap
  moviesJSONString.close()

  println("MOVIES", movies.take(5))

  // Wczytanie informacji o tym kto gdzie grał
  val actsFile = "./cineasts/acts_in.json"
  val actsJSONString = Source.fromFile(actsFile)
  val acts = actsJSONString
    .getLines()
    .map(act =>
      (parse(act).extract[ActsIn].to_id, parse(act).extract[ActsIn].from_id) -> parse(act).extract[ActsIn]
    )
    .toMap
  actsJSONString.close()


  println("ACTS", acts.take(5))

  // Połączenie w trójki (osoba1 (ID), osoba2 (ID), film (ID))

  var tripletsList = new ListBuffer[Connection]()
  var i = 0 // TODO delete this counting later

  movies.keys.foreach( movie_id=>
            {
                  if (i%500 == 0) println( "%d percent done".format( {i*100/movies.keys.size} ))

                  val acts_filtered_by_movie = acts.values.filter(_.to_id == movie_id)

                  acts_filtered_by_movie.foreach(
                      act1 => acts_filtered_by_movie.foreach(
                      act2 => {tripletsList += Connection(act1.from_id,act2.from_id, movie_id)}))

                  i+=1
            }
  )

  println(tripletsList.take(5))


  // Elminowanie duplikatów par osób - jedno połączenie wystarczy - może reduceByKey czy jakoś tak
  //  val reducedConnections = x
  //    .groupBy(_._1)
  //    .reduce((a, b) => a)
  //

  // Wygenerowanie grafu z wierzchołkami jako ludźmi i krawędziami jako grał z ... w ...
  //  val graph = Graph.(people, movies)
}
