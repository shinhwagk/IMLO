import java.util.UUID

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import akka.stream.scaladsl.{Sink, Source}

/**
  * Created by gk on 2015/10/19.
  */
object server extends App {
  implicit val system = ActorSystem("OracleImportCassandra")
  implicit val materializer = ActorMaterializer()
  val pub = system.actorOf(Props[A])
  val source = ActorPublisher(pub)
  val sub = system.actorOf(Props[B])
  val sink = ActorSubscriber[Int](sub)
  Source(source).runWith(Sink(sink))
}


class A extends ActorPublisher[Int] {
  var a = 0

  override def receive: Receive = {
    case Request(cnt) =>
      while (totalDemand > 0 && isActive) {

        if (a == 10) {
          context.system.shutdown()
        }else {
          a += 1
          onNext(a)
        }
        println(a + "bbb")

      }
  }
}

class B extends ActorSubscriber {
  override def receive: Receive = {
    case OnNext(i: Int) => {
      println(s"$i aaaa")
    }
    case onComplete =>
      println("okokoko")

  }

  override protected def requestStrategy: RequestStrategy = WatermarkRequestStrategy(30)
}