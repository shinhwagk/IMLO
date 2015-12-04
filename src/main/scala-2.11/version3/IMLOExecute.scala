package version3

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import akka.stream.scaladsl.{Sink, Source}

/**
  * Created by gk on 2015/12/3.
  */
object IMLOExecute extends App {
  implicit val system = ActorSystem("OracleImportCassandra")
  implicit val materializer = ActorMaterializer()

  val IMLOSource = ActorPublisher[String](system.actorOf(Props[IMLOSource]))
  val IMLOSink = ActorSubscriber[String](system.actorOf(Props[IMLOSink]))

  Source(IMLOSource).to(Sink(IMLOSink)).run()
}
