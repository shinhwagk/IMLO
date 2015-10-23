package org.gk.imlo.stream.subscriber

import akka.actor._
import akka.routing.RoundRobinPool
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy}
import com.datastax.driver.core.ResultSetFuture
import org.gk.imlo.Message
import org.gk.imlo.Message.{TbllogRow, ImportRowToCassandraSuccess, SuccessAtid}
import org.gk.imlo.control.MetaDataSource
import org.gk.imlo.target.TargetSlaveCassandra


class TargetSubscriber extends ActorSubscriber with ActorLogging {

  val MaxQueueSize = 20 * 5000 * 2

  override val requestStrategy = new MaxInFlightRequestStrategy(max = MaxQueueSize) {

    override def inFlightInternally: Int = rowRsMap.map(_._2._2.size).sum

    override def batchSize = 5000
  }

  val targetActorRoutees = context.actorOf(RoundRobinPool(1, supervisorStrategy = supervisorStrategy).props(Props[TargetSlaveCassandra]), "targetActorRoutees")

  var startTime = System.nanoTime()
  var totalAmount = 0l
  val metaData = new MetaDataSource
  var insertCount = 0

  import scala.collection.mutable.{Map, Set}

  val rowRsMap = Map[Long, (Int,Set[ResultSetFuture])]()

  def receive = {
    case OnNext(row: TbllogRow) =>
      targetActorRoutees ! row
      val aTid = row.TID / 5000 * 5000
      if (!rowRsMap.contains(aTid))
        rowRsMap += (aTid ->(metaData.getCount(aTid), Set[ResultSetFuture]()))

    case OnError(err: Exception) =>
      println(err, "[FibonacciSubscriber] Receieved Exception in Fibonacci Stream")
      context.stop(self)

    case OnComplete =>
      println("[FibonacciSubscriber] Fibonacci Stream Completed!")
      context.stop(self)

    case ImportRowToCassandraSuccess(rs, tid) =>
      insertCount = rowRsMap.map(_._2._2.size).sum
      rowRsMap.get(tid / 5000 * 5000).get._2 += rs

      rowRsMap.foreach { p =>
        val sourceCount = p._2._1
        val currSetSize = p._2._2.size
        val noSucessSet = p._2._2.filter(!_.isDone)
        //得到一个未完成的集合，通过他获得未完成的size，用当前的集合size 减去未完成的集合size，得到完成的数量，用应该完成的数量减去完成数量，来减少set储存已经完成的rs。
        val newCount = sourceCount - (currSetSize - noSucessSet.size)
        if (newCount == 0) {
          rowRsMap -= p._1
          self ! SuccessAtid(p._1)
        } else {
          rowRsMap += (p._1 ->(newCount, noSucessSet))
        }
      }

      if (System.nanoTime() - startTime >= 1000000000L) {
        println("每秒插入记录数: " + (rowRsMap.map(_._2._2.size).sum - insertCount) + ". 当前已插入总数量: " + totalAmount + ". 剩余处理数量" + rowRsMap.map {
          _._2._2.size
        }.sum + "/" + rowRsMap.size + " 经过时间" + (System.nanoTime() - startTime) / 1000000)
        startTime = System.nanoTime()
      }
      totalAmount += 1

    case SuccessAtid(atid) =>
//      metaData.clearATid(atid)
      println(rowRsMap.map(_._2._1).sum + "当前存在")
      println(atid + "完成啦....")
  }
}