import javax.management.Attribute

import akka.actor.Actor.Receive
import akka.actor.{Props, ActorSystem}
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.{ActorMaterializerSettings, OverflowStrategy, Attributes, ActorMaterializer}
import akka.stream.scaladsl.{Sink, Flow, Source}
import com.typesafe.config.ConfigFactory
import org.gk.imlo.control.Checkpoint
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Created by gk on 2015/10/29.
 */
object test77 extends App {
  implicit val system = ActorSystem("ClusterSystem",ConfigFactory.load())
  implicit val materializer = ActorMaterializer(
//    ActorMaterializerSettings(system)
//      .withInputBuffer(
//        initialSize = 32,
//        maxSize = 32)
  )
//      val source = Source(1 to 20)
//  val source = Source(ActorPublisher[Int](system.actorOf(Props[P])))
//  //    .buffer(10,OverflowStrategy.backpressure)
//  //    .buffer(10,OverflowStrategy.dropBuffer)
////      .withAttributes(Attributes.inputBuffer(128,128))
  val flow = Flow[Int].mapAsync(10){ p =>
    Future{println(s"flow $p");Thread.sleep(10000);p}
  }
  .withAttributes(Attributes.inputBuffer(2,64))
  .buffer(100,OverflowStrategy.backpressure)

////    .buffer(10000,OverflowStrategy.backpressure)
////  .buffer(1000,OverflowStrategy.backpressure)
//  //    .withAttributes(Attributes.inputBuffer(128,128))
////        .buffer(100,OverflowStrategy.backpressure)
//
//  val sink = Sink.foreach[Int](p=>{println(s"sink $p");Thread.sleep(10000000);println(p)})
//    .withAttributes(Attributes.inputBuffer(128,128))
//  val f2 =Flow[Int].mapAsyncUnordered(1000){x=>Future{println("pp");Thread.sleep(100000);x}}

//  source.via(flow).runWith(Sink(ActorSubscriber[Int](system.actorOf(Props[Sub]))))
//  ActorPublisher[Int](system.actorOf(Props[P])).subscribe(ActorSubscriber[Int](system.actorOf(Props[Sub])))

  Source(ActorPublisher[Int](system.actorOf(Props[P]))).via(flow).runWith(Sink(ActorSubscriber[Int](system.actorOf(Props[Sub]))))
}

class P extends ActorPublisher[Int] {
  var num = 0

  override def receive: Receive = {
    case Request(cnt) => {
      println(s"请求CNT数量 $cnt", totalDemand)
        (1l to totalDemand).foreach(p => {
          num += 1
          onNext(num)
        })
    }
  }
}

class Sub extends ActorSubscriber{
//
  var a  = 0
  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(max = 8) {
    override def inFlightInternally: Int = a
  }
//val requestStrategy = WatermarkRequestStrategy(50)
  override def receive: Receive = {
    case OnNext(id:Int) =>{
      a +=1
      Thread.sleep(10000)
      println("准备保存")
      a-=1
    }
  }
}
