package test

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core.Cluster
import org.gk.imlo.Message.RowsInfo

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

/**
  * Created by gk on 2015/11/10.
  */
object Execute extends App {
  implicit val system = ActorSystem("OracleImportCassandra")
  implicit val materializer = ActorMaterializer()

  MetaDB.initTaskInfo(2)
  val task = new Task
  val sourcePubActor = system.actorOf(Props(new SourcePublisher(task)), name = "source")
  val targetSubscriber = system.actorOf(Props(new TargetSubscriber(task)), name = "target")
  val sourcePub = ActorPublisher[RowsInfo](sourcePubActor)
  val targetSub = ActorSubscriber[RowsInfo](targetSubscriber)
  Source(sourcePub).to(Sink(targetSub)).run()
}

class SourcePublisher(task: Task) extends ActorPublisher[RowsInfo] with ActorLogging {

  var count = if (MetaDB.getStartTaskKeyNum == 0) MetaDB.checkpoint + 1 else MetaDB.getStartTaskKeyNum + 1
  val unSuccessQueue = MetaDB.unSuccessQueue
  val maxKeyNum = MetaDB.maxNumber
  val systemSelf = context.system
  override def receive: Receive = {
    case Request(cnt) =>

      println(s"Source: , 收到请求row数量:$cnt......................")
      (1l to cnt.toInt).foreach { p =>
        if (unSuccessQueue.size > 0) {
          val keyNum = unSuccessQueue.dequeue()
          context.watch(context.actorOf(Props(classOf[SourceSlave], task), name = "source-" + keyNum.toString)).tell(keyNum, self)
        } else if (count <= maxKeyNum) {
          MetaDB.insertChildCheckpoint(count)
          context.watch(context.actorOf(Props(classOf[SourceSlave], task), name = "source-" + count.toString)).tell(count, self)
          count += 1
        } else {
          Logger.info("源已经结束...")
        }
      }

    case rowsInfo: RowsInfo =>
      if (isActive && totalDemand > 0) {
        onNext(rowsInfo)
        println(rowsInfo.key / 5000, "发送cassanra已经送出")
      }
      context.unwatch(sender())
      context.stop(sender())
  }

}

class SourceSlave(task: Task) extends Actor {

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println("actor:" + self.path + ",preRestart child, reason:" + reason + ", message:" + message)
    println("重新发送至:" + self.path + ". 父:" + sender().path)
    val keyNum = message.get.asInstanceOf[Int]
    self.tell(keyNum, sender()) //
  }

  override def receive: Receive = {
    case keyNum: Int =>
      println("slaveshoudao key", keyNum, self.path)
      sender() ! task.pullRowSet(keyNum)
  }
}

class TargetSubscriber(task: Task) extends ActorSubscriber with ActorLogging {

  val actorRefBuffer = ArrayBuffer(
    context.watch(context.actorOf(Props(new TargetSlave("192.168.20.181", task)))),
    context.watch(context.actorOf(Props(new TargetSlave("192.168.20.182", task)))),
    context.watch(context.actorOf(Props(new TargetSlave("192.168.20.183", task)))),
    context.watch(context.actorOf(Props(new TargetSlave("192.168.20.184", task))))
  ).toArray

  var count: Int = 0

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(50) {
    override def inFlightInternally: Int = count
  }

  override def receive: Receive = {
    case OnNext(rowsInfo: RowsInfo) =>
      count += 1
      println("收到", (rowsInfo.key / 5000).toInt, count)
      actorRefBuffer(Random.nextInt(4)).tell(rowsInfo, self)

    case keyNum: Long =>
      println("checkpoint", keyNum)
      count -= 1
      MetaDB.updateChildCheckpoint(keyNum.toInt / 5000)
      println("升级为处理完成数量", count, (keyNum / 5000).toInt)
  }
}

class TargetSlave(ip: String, task: Task) extends Actor {
  override def preRestart(reason: Throwable, message: Option[Any]) {
    println("actor:" + self.path + ",preRestart child, reason:" + reason + ", message:" + message)
    println("重新发送至:" + self.path + ". 父:" + sender().path)
    val rowsInfo = message.get.asInstanceOf[RowsInfo]
    //    val rowsInfo  =task.getRowSet(keyNum)
    self.tell(rowsInfo, sender())
  }

  val keySpace = MetaDB.getTargetCassandraInfo
  val cluster = Cluster.builder().addContactPoints(ip).build();
  val session = cluster.connect(keySpace)

  override def receive: Receive = {
    case rowsInfo: RowsInfo => {
      sender() ! task.pushRowSet(ip, rowsInfo, session)
    }
  }
}



