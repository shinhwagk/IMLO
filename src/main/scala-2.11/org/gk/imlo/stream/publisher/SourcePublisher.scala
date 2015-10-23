package org.gk.imlo.stream.publisher

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.routing.RoundRobinPool
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import org.gk.imlo.Message.RowInfo
import org.gk.imlo.control.MetaDataSource
import org.gk.imlo.source.{SourceActuator, SourceSlaveOracle}


class SourcePublisher extends ActorPublisher[RowInfo] with ActorLogging {


  override def receive: Receive = {

    case Request(cnt) =>

      println(s"Source: , 收到请求row数量:${cnt}")

      for (i <- 1 to (cnt / 5000).toInt) {
//        val aTid = metaData.getATid
//        println("接收到" + aTid)
//        if (aTid == None) {
//          self ! Cancel
//          println("导入结束....")
//        }
//        println("接收到2" + aTid)
//        sourceActorRoutees ! aTid.get
      }

    case Cancel =>
      println("[FibonacciPublisher] Cancel Message Received -- Stopping")
      context.stop(self)

    case row: RowInfo =>
      if (isActive && totalDemand > 0) {
        onNext(row)
      }
  }
}










