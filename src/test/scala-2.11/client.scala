import akka.actor.Actor.Receive
import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import akka.stream.actor.{RequestStrategy, ActorSubscriber}
import akka.stream.{ActorAttributes, Supervision, OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}
import akka.util.ByteString
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.{PushStage, SyncDirective, Context, PushPullStage}

/**
 * Created by gk on 2015/10/19.
 */
object client extends App {
    implicit val system = ActorSystem("ClientAndServer")
    import system.dispatcher
    implicit val materializer = ActorMaterializer()




    val source = Source(List(1, 2, 3, 3, 3, 4, 5, 5, 6, 6, 6, 6, 6, 7, 6))
    val sink = Sink.foreach(println)

    source.transform(() => new DistinctStage[Int]).runWith(sink)
      .onComplete(_ => system.shutdown)


  class DistinctStage[A] extends PushStage[A, A] {

    private[this] var lastSeen: Option[A] = None

    def onPush(elem: A, ctx: Context[A]): SyncDirective = {
      if (lastSeen.contains(elem)) {
        println(s"already know $elem")
        ctx.pull()
      } else {
        println(s"Whohoo, new element $elem")
        lastSeen = Some(elem)
        ctx.push(elem)
      }
    }

  }


}
