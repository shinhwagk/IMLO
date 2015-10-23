package org.gk.imlo

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorSubscriber
import akka.stream.scaladsl._
import org.gk.imlo.Message.RowsInfo
import org.gk.imlo.stream.subscriber.TargetSubscriber

import scala.collection.mutable.ArrayBuffer

/**
 * Created by gk on 2015/10/12.
 */
object Main {


  def main(args: Array[String]) {

    implicit val system = ActorSystem("ClusterSystem")
    implicit val materializer = ActorMaterializer()

    //    val publisher = ActorPublisher[Long](system.actorOf(Props[SourcePublisher], name = "source"))
    val subscriber = ActorSubscriber[Long](system.actorOf(Props[TargetSubscriber], name = "target"))

    val in = Source(for (i <- 1l to 10000l) yield i * 10)

    val flowMark = Flow[Long].map { p => p }

    val flowGetRows = Flow[Long].map { p => p; RowsInfo(1l, ArrayBuffer(Array(("a", "a")))) }

    //过滤掉没有数据的key范围
    val flowRowsFilter1 = Flow[RowsInfo].filter(_.rows.size > 0)

    val flowRowsFilter2 = Flow[RowsInfo].filter(_.rows.size == 0).map(_.key)

    //导入Cassandra
    val flowImC = Flow[RowsInfo].map { p => p.key }

    val success = Sink.foreach[Long]{p=>    }

    val g = FlowGraph.closed() { implicit builder: FlowGraph.Builder[Unit] =>

      import FlowGraph.Implicits._

      val bcast = builder.add(Broadcast[RowsInfo](2))

      val merge = builder.add(Merge[Long](2))

      in ~> flowMark ~> flowGetRows ~> bcast ~> flowRowsFilter1 ~> flowImC ~> merge ~> success
      //
      bcast ~> flowRowsFilter2 ~> merge
    }.run()
  }
}



