package test.meta

import java.sql.DriverManager

import slick.driver.MySQLDriver.api._
import slick.lifted.ProvenShape

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by gk on 2015/11/9.
  */
object MetaDB {
  def getUrl(ip: String, port: String, dbname: String, username: String, password: String) = {
    s"jdbc:mysql://$ip:$port/$dbname?user=$username&password=$password"
  }

  val url = MetaDB.getUrl("192.168.20.184", "3306", "import", "import", "000000")
  val conn = DriverManager.getConnection(url);
  var sourceId = 0
  var targetId = 0
  var taskId = 0
  var checkpoint = 0

  def getTaskInfo(taskId: Int) = {
    val sql = s"select * from task where id = $taskId"
    val rs = conn.createStatement().executeQuery(sql)
    rs.next()

    sourceId = rs.getInt(2)
    targetId = rs.getInt(3)
    checkpoint = rs.getInt(4)
    this.taskId = taskId
    rs.close
  }

  case class InfoJdbc(id: Int, ip: String, port: String, service: String, username: String, password: String, tablename: String, primarykey: String, columns: String, whereString: String, step: Int)

  def getInfoJdbc = {
    val sql = s"select * from infojdbc where id = $sourceId"
    val rs = conn.createStatement().executeQuery(sql)
    rs.next()
    val infoJdbc = InfoJdbc(
      rs.getInt("id"),
      rs.getString("ip"),
      rs.getString("port"),
      rs.getString("service"),
      rs.getString("username"),
      rs.getString("password"),
      rs.getString("tablename"),
      rs.getString("primarykey"),
      rs.getString("columns"),
      rs.getString("wherestring"),
      rs.getInt("step")
    )
    rs.close
    infoJdbc
  }

  //
  //  def getInfoCassandra = {
  //    Await.result(metaDB.run(MetaTable.infoJdbc.filter(_.Id === targetId).result.head), Duration.Inf).copy()
  //  }

  def updateTaskCheckpoint(checkpoint: Int) = {
    val sql = s"update task set checkpoint = $checkpoint where id = $taskId";
    conn.createStatement().execute(sql)
  }

  def getCurrentCheckpoint = {
    val sql = s"select checkpoint from task where id = $taskId"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    rs.next
    val checkpoint = rs.getInt(1)
    rs.close
    stmt.close
    checkpoint
  }

  def getUnfinishedchildTask = {
    val sql = s"select count(*) from childtask where success = 0 and taskid=$taskId and execing = 0"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    rs.next()
    val count = rs.getInt(1)
    rs.close()
    stmt.close
    count

  }

  def getMaxChildTaskKeyNum = {
    val sql = s"select max(keynum) from childtask"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    rs.next()
    val maxKeyNum = rs.getInt(1)
    rs.close()
    stmt.close()
    maxKeyNum
  }

  def getMinChildTaskKeyNum = {
    val sql = s"select min(keynum) from childtask where success = 1"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    rs.next()
    val minKeyNum = rs.getInt(1)
    rs.close()
    stmt.close()
    minKeyNum
  }

  def makeNewKeyNum(keyNum: Int) = {
    val sql = s"insert into childtask values($taskId,$keyNum,0,0)"
    val stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close()

    val stmt2 = conn.createStatement()
    stmt2.executeUpdate(s"update childtask set execing = 1 where taskid = $taskId and keynum = $keyNum")
    stmt2.close

  }

  def getOnceUnfinishedchildTask: Int = {
    val sql = s"select min(keynum) from childtask where taskid = $taskId and execing = 0 and success = 0"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    rs.next()
    val min = rs.getInt(1)
    rs.close()
    stmt.close

    val stmt2 = conn.createStatement()
    stmt2.executeUpdate(s"update childtask set execing = 1 where taskid = $taskId and keynum = $min")
    stmt2.close

    min
  }

  def updateChildCheckpoint(keyNum: Int) = {
    val sql = s"update childtask set success = 1 where taskid = $taskId and execing = 1 and keynum = $keyNum"
    val stmt = conn.createStatement
    stmt.executeUpdate(sql)
    stmt.close
  }


  def updateCheckpoint(keyNum: Int) = {
    val sql = s"update task set checkpoint = $keyNum where id = $taskId and checkpoint = $keyNum - 1"
    val stmt = conn.createStatement
    val count = stmt.executeUpdate(sql)
    stmt.close
    println(count,"......")
    count
  }

  def updateChildExecing = {
    val sql = s"update childtask set execing=0 where success =0"
    val stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close
  }

  def deleteChildTask(keyNum:Int) ={
    val stmt = conn.createStatement()
    stmt.executeUpdate(s"delete from childtask where keynum = $keyNum")
    stmt.close()
  }

  //  def insertCheckpointList(keyNum: Int): Unit = {
  //    val sql = s"insert into checkpointlist values($keyNum)"
  //    conn.createStatement().execute(sql)
  //  }
}
