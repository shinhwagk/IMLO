package test

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.routing.RoundRobinPool
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import akka.stream.scaladsl.{Sink, Source}
import org.gk.imlo.Message.RowsInfo
import org.gk.imlo.source.SourceSlaveOracle
import test.meta.MetaDB
import test.target.Cassandra
import test.task.Task

import scala.collection.mutable

/**
  * Created by gk on 2015/11/10.
  */
object Execute extends App {
  implicit val system = ActorSystem("OracleImportCassandra")
  implicit val materializer = ActorMaterializer()

  val task = new Task(1)
  val sourcePubActor = system.actorOf(Props(new SourcePublisher(task)), name = "source")
  val targetSubscriber = system.actorOf(Props(new TargetSubscriber(task)), name = "target")
  val sourcePub = ActorPublisher[RowsInfo](sourcePubActor)
  val targetSub = ActorSubscriber[RowsInfo](targetSubscriber)
  Source(sourcePub).to(Sink(targetSub)).run()
}

class SourcePublisher(task: Task) extends ActorPublisher[RowsInfo] with ActorLogging {
  //  val slave = context.actorOf(Props(classOf[SourceSlave], task), "makeRowSetslave")

//  override val supervisorStrategy = OneForOneStrategy() {
//    case _: Exception => Restart
//  }

  val slave = context.actorOf(Props(classOf[SourceSlave], task), "makeRowSetslave")

  override def receive: Receive = {
    case Request(cnt) =>
      println(s"Source: , 收到请求row数量:$cnt")
      (1l to cnt).foreach { p =>
        slave.tell(task.makeNewExecBuffer,self)
      }

    case rowsInfo: RowsInfo =>
      if (isActive && totalDemand > 0) {
        onNext(rowsInfo)
      }
  }

}

class SourceSlave(task: Task) extends Actor {
  override def preRestart(reason: Throwable, message: Option[Any]) {
    println("actor:" + self.path + ",preRestart child, reason:" + reason + ", message:" + message)
    println("重新发送至:" + self.path + ". 父:" + sender().path)
    val keyNum = message.get.asInstanceOf[Int]
//    val rowsInfo  =task.getRowSet(keyNum)
    self.tell(keyNum,sender())
//    System.exit(1)
  }

  override def receive: Receive = {
    case keyNum:Int =>
      sender() ! task.getRowSet(keyNum)
  }
}

class TargetSubscriber(task: Task) extends ActorSubscriber with ActorLogging {
  val cassnadra = new Cassandra

  override protected def requestStrategy: RequestStrategy = WatermarkRequestStrategy(30)

  override def receive: Receive = {
    case OnNext(rowsInfo: RowsInfo) =>
      println("收到", rowsInfo.key)
      val keyNum = cassnadra.exec(rowsInfo)
      println("checkpoint",rowsInfo.key)
      task.updateSuccessKeyNum(keyNum.toInt)
  }
}