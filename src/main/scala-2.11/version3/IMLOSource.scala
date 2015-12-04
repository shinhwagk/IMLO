package version3

import java.sql.Connection

import akka.actor.{Props, Actor}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import com.google.gson.Gson
import com.zaxxer.hikari.HikariDataSource

/**
  * Created by gk on 2015/12/3.
  */
class IMLOSource extends ActorPublisher[String] {
  val ds = new HikariDataSource();
  ds.setJdbcUrl("jdbc:oracle:thin:@192.168.12.42:1521/pdb_log");
  ds.setUsername("log");
  ds.setPassword("log");
  ds.setMaximumPoolSize(50)
  var id: Long = 146788795l

  override def receive: Receive = {
    case Request(cnt) =>
      println(s"Source: , 收到请求row数量:$cnt.......................")
      (1l to cnt).foreach { p =>
        val conn = ds.getConnection()
        context.actorOf(Props[SourceOracle]).tell((id, conn), self)
        id += 1l
      }
    case json: String =>
      context.stop(sender())
      onNext(json)
  }
}

class SourceOracle extends Actor {
  val gson = new Gson();

  override def receive: Receive = {
    case (id: Long, conn: Connection) =>
      val stmt = conn.prepareStatement("select * from usa_tbldownupdatelog where id = ?")
      stmt.setLong(1, id)
      val rs = stmt.executeQuery()
      while (rs.next()) {
        for (i <- 1 to rs.getMetaData.getColumnCount) {
          sender() ! gson.toJson(TblDownUpdateLog(rs.getLong("id"), rs.getString("imsi")))
        }
      }
      rs.close()
      stmt.close()
      conn.close()
  }
}

case class TblDownUpdateLog(val id: Long, val imsi: String)

