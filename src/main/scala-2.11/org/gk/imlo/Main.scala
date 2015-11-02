package org.gk.imlo

import java.util.Date

import akka.actor.{ActorSystem, Props}
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import akka.stream.scaladsl._
import akka.stream.{OverflowStrategy, ActorMaterializer, Attributes}
import com.datastax.driver.core.{Cluster, ResultSetFuture}
import org.gk.imlo.Message.RowsInfo
import org.gk.imlo.control.ImportTable
import org.gk.imlo.stream.publisher.SourcePublisher
import slick.driver.MySQLDriver.api._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Created by gk on 2015/10/12.
 */
object Main {
  val db = Database.forURL("jdbc:mysql://192.168.20.184:3306/import?user=import&password=000000", driver = "com.mysql.jdbc.Driver",
    executor = AsyncExecutor("test1", numThreads = 100, queueSize = 5000))

  def main(args: Array[String]) {
    implicit val system = ActorSystem("ClusterSystem")
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

    //过滤掉没有数据的key范围
    val flowRowsFilter1 = Flow[RowsInfo].filter(p => {
      //      println("过滤1 " + p.key);
      p.rows.size > 0
    })

    val flowRowsFilter2 = Flow[RowsInfo].filter(p => {
      //      println("过滤2 " + p.key);
      p.rows.size == 0
    }).map(_.key)

    //导入Cassandra
    val cluster = Cluster.builder().addContactPoint("192.168.20.181").build();
    val session = cluster.connect("bigdata")
    val cql = "insert into TBLDOWNUPDATELOG " +
      "(ID,SERIALNO,HW,HWV,SWV,PREVERSION,CURVERSION,UPDATESTATUS,CREATEDATE,BDT,CUST,KERNEL,MAF,BOARD,LANG,NET,OPRATOR,SMSC,LOGTYPE,AID,IMSI)" +
      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val prepared = session.prepare(cql)
    val bound_stmt = prepared.bind()

    import scala.collection.mutable.Set


//    val rsetSet = ArrayBuffer[ResultSetFuture]()
    val flowImportCassandra = Flow[RowsInfo].mapAsyncUnordered(threadNum) { p =>
      Future {
        val rsetSet = Set[ResultSetFuture]()
        for (r <- p.rows) {
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
          val rsf = session.executeAsync(bound_stmt)
          rsetSet += rsf
        }
        while (rsetSet.filterNot(_.isDone).size != 0)
        //          println(rsetSet.filterNot(_.isDone).size, s"${p.key}  等待插入cassandra完成")
          Thread.sleep(50)

        p.key
      }
    }.buffer(100,OverflowStrategy.backpressure)

    val success = Sink(ActorSubscriber[Long](system.actorOf(Props[Sub])))

    FlowGraph.closed() { implicit builder: FlowGraph.Builder[Unit] =>

      import FlowGraph.Implicits._

      val bcast = builder.add(Broadcast[RowsInfo](2).withAttributes(Attributes.inputBuffer(128, 128)))
      val merge = builder.add(Merge[Long](2))

      in ~> listIn

      listOut ~> bcast ~> flowRowsFilter1 ~> flowImportCassandra ~> merge ~> success

      bcast ~> flowRowsFilter2 ~> merge

    }.run()

  }
}


class Sub extends ActorSubscriber {
  var successCount = 0

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(max = 5000) {
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


