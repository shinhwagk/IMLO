package org.gk.imlo.stream.publisher

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.routing.RoundRobinPool
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import org.gk.imlo.Message.RowsInfo
import org.gk.imlo.source.SourceSlaveOracle

import scala.collection.mutable

class SourcePublisher(threadNum: Int) extends ActorPublisher[RowsInfo] with ActorSubscriber with ActorLogging {

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(max = 5) {
    override def inFlightInternally: Int = queue.size
  }

  val queue = mutable.Queue[RowsInfo]()
  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => Restart
  }

  val slave = context.actorOf(RoundRobinPool(threadNum, supervisorStrategy = supervisorStrategy).props(Props[SourceSlaveOracle]), "sourceDataSlave")

  override def receive: Receive = {

    case Request(cnt) =>
      println(s"Source: , 收到请求row数量:${
        cnt
      }")

      while (isActive && totalDemand > 0 && !queue.isEmpty) {
        onNext(queue.dequeue())
      }

    case Cancel =>
      println("[FibonacciPublisher] Cancel Message Received -- Stopping")
      context.stop(self)

    case rowsInfo: RowsInfo =>
      println("当前queue中的数量", queue.size)
      queue += rowsInfo
      while (isActive && totalDemand > 0 && !queue.isEmpty) {
        onNext(queue.dequeue())
      }

    case OnNext(startId: Int) =>
      slave.tell((startId * 5000).toLong, self)

  }

}










