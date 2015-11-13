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
  private var sourceId = 0
  private var targetId = 0
  private var taskId = 0
  var checkpoint = 0

  var step = 0

  var sourceConnString:String = _
  var sourceUsername:String = _
  var sourcePassword:String = _

  var targetIp:String = _
  var targetKeyspace:String = _

  var sql: String = _
  var cql: String = _

  def initTaskInfo(taskId: Int) = {
    val query = s"select * from TaskOracleToCassandra where id = $taskId"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(query)
    rs.next()
    sourceId = rs.getInt(2)
    targetId = rs.getInt(3)
    checkpoint = rs.getInt(7)
    sql = rs.getString(4)
    cql = rs.getString(5)
    step = rs.getInt(6)
    this.taskId = taskId
    rs.close
    stmt.close()
    getSourceOracle
    getTargetCassandra
  }


  def getSourceOracle = {
    val query = s"select * from SourceOracle where id = $sourceId"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(query)
    rs.next()
    sourceConnString = rs.getString("connString")
    sourceUsername = rs.getString("username")
    sourcePassword = rs.getString("password")
    rs.close
    stmt.close
  }

  def getTargetCassandra = {
    val query = s"select * from TargetCassandra where id = $targetId"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(query)
    rs.next()
    targetIp = rs.getString("ip")
    targetKeyspace = rs.getString("keyspace")
    rs.close
    stmt.close
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
    val sql = s"update TaskOracleToCassandraCheckpoint set endTime = now() where taskId = $taskId and keyNum = $keyNum"
    val stmt = conn.createStatement
    stmt.executeUpdate(sql)
    stmt.close
  }

  def insertChildCheckpoint(keyNum: Int) = {
    val sql = s"insert into TaskOracleToCassandraCheckpoint values($taskId,$keyNum,now(),null)"
    val stmt = conn.createStatement
    stmt.executeUpdate(sql)
    stmt.close
  }

  def updateChildExecing = {
    val sql = s"update childtask set execing=0 where success =0"
    val stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close
  }

  def deleteChildTask(keyNum: Int) = {
    val stmt = conn.createStatement()
    stmt.executeUpdate(s"delete from childtask where keynum = $keyNum")
    stmt.close()
  }
}
