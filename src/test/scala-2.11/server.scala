import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl.Tcp.{ServerBinding, IncomingConnection}
import akka.stream.scaladsl.{Flow, Tcp, Sink, Source}
import akka.util.ByteString
import akka.stream.io.Framing
import scala.util.{Failure, Success}
import scala.concurrent.Future
import scala.util.Success

/**
 * Created by gk on 2015/10/19.
 */
object server extends App {
  //  implicit val system = ActorSystem("ClientAndServer")
  //  import system.dispatcher
  //  implicit val materializer = ActorMaterializer()
  //
  //  val handler = Sink.foreach[Tcp.IncomingConnection] { conn =>
  //      println("Client connected from: " + conn.remoteAddress)
  //      conn handleWith Flow[ByteString]
  //    }
  //
  //  val connections = Tcp().bind("127.0.0.1", 2222)
  //  val binding = connections.to(handler).run()
  //
  //  binding.onComplete {
  //    case Success(b) =>
  //      println("Server started, listening on: " + b.localAddress)
  //    case Failure(e) =>
  //      println(s"Server could not bind to  : ${e.getMessage}")
  //      system.shutdown()
  //  }


  try {
    throw new Exception("a")
  }
  finally {
    println("x")
  }

}

