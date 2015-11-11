package org.gk.imlo

import java.util.Date

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{OneForOneStrategy, Actor, ActorSystem, Props}
import akka.routing.RoundRobinPool
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import akka.stream.scaladsl._
import akka.stream.{OverflowStrategy, ActorMaterializer, Attributes}
import com.datastax.driver.core._
import org.gk.imlo.Message.RowsInfo
import org.gk.imlo.control.ImportTable
import org.gk.imlo.source.SourceSlaveOracle
import org.gk.imlo.stream.publisher.SourcePublisher
import slick.driver.MySQLDriver.api._
import test.meta.MetaDB

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Created by gk on 2015/10/12.
 */
object Main extends {

  val db = Database.forURL("jdbc:mysql://192.168.20.184:3306/import?user=import&password=000000", driver = "com.mysql.jdbc.Driver",
    executor = AsyncExecutor("test1", numThreads = 100, queueSize = 5000))
  //  MetaDB.getMetaDB("192.168.20.184","3306","import","import","000000")

  def main(args: Array[String]) {
//    val taskId = args(0).toInt
//    MetaDB.getJdbcInfo(taskId)


    implicit val system = ActorSystem("OracleImportCassandra")
    implicit val materializer = ActorMaterializer()
    val q = for (c <- ImportTable.checkpointTab.filter(_.SUCCESS === 0).sortBy(_.START_ID)) yield c.START_ID
    val a = q.result
    val unSuccessList = db.stream(a)

    val threadNum = 20
    val arf = system.actorOf(Props(new SourcePublisher(threadNum)), name = "source")
    val actorSub = ActorSubscriber[Int](arf)
    val actorPub = ActorPublisher[RowsInfo](arf)

    val in = Source(unSuccessList)
    val listIn = Sink(actorSub)
    val listOut = Source(actorPub)

    val imArf = system.actorOf(Props[ImportCassandraSub])
    val imActorSub = ActorSubscriber[RowsInfo](imArf)
    val imActorPub = ActorPublisher[Long](imArf)
    val imListIn = Sink(imActorSub)
    val imListOut = Source(imActorPub)
    //导入Cassandra

    val success = Sink(ActorSubscriber[Long](system.actorOf(Props[Sub])))

    FlowGraph.closed() { implicit builder: FlowGraph.Builder[Unit] =>

      import FlowGraph.Implicits._

      in ~> listIn
      listOut ~> imListIn
      imListOut ~> success
    }.run()
  }
}

class ImportCassandraSub extends ActorSubscriber with ActorPublisher[Long] {
  val queue = mutable.Queue[Long]()
  var count = 0

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(max = 5) {
    override def inFlightInternally: Int = count
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => Restart
  }

  val slave = context.actorOf(Props[ExectuerImport], "exectuerImport")

  override def receive: Receive = {
    case Request(cnt) =>
      println(s"Source: , 收到请求row数量IMPORT:${
        cnt
      }")
      while (isActive && totalDemand > 0 && queue.size > 0) {
        count -= 1
        onNext(queue.dequeue())
      }
    case OnNext(rowsInfo: RowsInfo) => {
      slave.tell(rowsInfo, self)
      count += 1
      println("当前未完成导入cassandra的数量", count)
    }
    case key: Long => {
      queue += key

      while (isActive && totalDemand > 0 && queue.size > 0) {
        count -= 1
        onNext(queue.dequeue())
      }
    }
  }
}

class ExectuerImport extends Actor {
  val poolingOptions = new PoolingOptions()
  //  poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 10, 50).setConnectionsPerHost(HostDistance.REMOTE, 10, 50)
  val cluster = Cluster.builder().addContactPoints("192.168.12.41").build();
  //  val cql = "insert into TBLDOWNUPDATELOG " +
  //    "(ID,SERIALNO,HW,HWV,SWV,PREVERSION,CURVERSION,UPDATESTATUS,CREATEDATE,BDT,CUST,KERNEL,MAF,BOARD,LANG,NET,OPRATOR,SMSC,LOGTYPE,AID,IMSI)" +
  //    "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  val cql = "insert into TBLLOG (TID,UUID,IMSI,BRAND,MODEL,CHANNEL,PLAT,ANDROIDVER,SCREENSIZE,LANG,APPSTOREVER,PROVIDER,CONNECTIONMODE,GETLOCTYPE,LOCSTR,COUNTRY,PROVINCE,CITY,IPADDR,ACCESSTYPE,CURRPAGE,PROPAGE,PROCONTENT,APPID,OTHERPARAS,CREATED,PHONE,PRODUCT,SDK,DISPLAY,CODENAME,TCARDSIZE,RAM,CPUCLOCKSPEED,SOURCE,SMSCENTER,ENC,PVER,IMEI,PKG) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

  val session = cluster.connect("bigdataanalysis")

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println("actor:" + self.path + ",preRestart child, reason:" + reason + ", message:" + message)
    println("重新发送至:" + self.path + ". 父:" + sender().path)
    self.tell(message.get.asInstanceOf[RowsInfo], sender())
  }

  override def receive: Receive = {
    case rowsInfo: RowsInfo =>
      val startTime = System.currentTimeMillis()
      val rsetBuffer = new ArrayBuffer[ResultSetFuture]()
      for (r <- rowsInfo.rows) {
        val ps = session.prepare(cql)
        val bound_stmt = ps.bind()
        for (r0 <- r) {
          val colType = r0._1
          val colVal = r0._3
          val colName = r0._2
          colType match {
            case "Long" => bound_stmt.setLong(colName, colVal.asInstanceOf[Long])
            case "String" => bound_stmt.setString(colName, colVal.asInstanceOf[String])
            case "Timestamp" => bound_stmt.setDate(colName, colVal.asInstanceOf[Date])
          }
        }
        rsetBuffer += session.executeAsync(bound_stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
      }
      //      println(rowsInfo.key,"开始等待完成插入")
      for (i <- rsetBuffer) {
        val info = i.getUninterruptibly.getExecutionInfo
      }


      println(rowsInfo.rows.size, (System.currentTimeMillis() - startTime) / 1000, "每秒插入数量估计...")
      sender() ! rowsInfo.key
  }
}

class Sub extends ActorSubscriber {
  var successCount = 0

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(max = 2) {
    override def inFlightInternally: Int = successCount
  }

  override def receive: Receive = {
    case OnNext(id: Long) => {
      successCount += 1
      val start_id = (id / 5000).toInt
      val update = ImportTable.checkpointTab.filter(_.START_ID === start_id).map(_.SUCCESS).update(1)
      //      val update = sql"update checkpoint set success = 1 where start_id = $start_id".as[Int]

      val updateFuture = Main.db.run(update)
      val selfarf = self
      updateFuture onSuccess { case a => selfarf ! start_id; }
    }
    case suid: Int =>
      println("当前在队列中的数量", successCount)
      successCount -= 1
      println(s"$suid 插入完毕")
  }
}


