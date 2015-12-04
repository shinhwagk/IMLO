package test

import java.sql.DriverManager
import java.util.Properties

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

  var sourceConnString: String = _
  var sourceUsername: String = _
  var sourcePassword: String = _

  var targetIp: String = _
  var targetKeyspace: String = _

  var sql: String = _
  var cql: String = _


  lazy val maxNumber = getSourceMax

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

  def updateChildCheckpoint(keyNum: Int) = {
    conn.setAutoCommit(false)
    val query = s"update TaskOracleToCassandraCheckpoint set endTime = now() where taskId = $taskId and keyNum = $keyNum"
    val stmt = conn.createStatement
    stmt.executeUpdate(query)
    stmt.close

    val query2 = s"delete from TaskOracleToCassandraCheckpoint where taskId = $taskId and endTime is not null and keyNum != (select * from (select max(keyNum) from TaskOracleToCassandraCheckpoint where taskId = $taskId) tab)"
    val stmt2 = conn.createStatement()
    stmt2.executeUpdate(query2)
    stmt2.close()


    val query3 = s"update TaskOracleToCassandra set checkpoint = (select min(keyNum)-1 from TaskOracleToCassandraCheckpoint where taskId = $taskId) where id = $taskId"
    val stmt3 = conn.createStatement()
    stmt3.executeUpdate(query3)
    stmt3.close
    conn.commit()
    conn.setAutoCommit(true)
  }

  def insertChildCheckpoint(keyNum: Int) = {
    val query = s"insert into TaskOracleToCassandraCheckpoint values($taskId,$keyNum,now(),null)"
    val stmt = conn.createStatement
    stmt.executeUpdate(query)
    stmt.close
  }

  def unSuccessQueue: scala.collection.mutable.Queue[Int] = {
    val queue = scala.collection.mutable.Queue[Int]()
    val query = s"select keyNum from TaskOracleToCassandraCheckpoint where endTime is null and taskId = $taskId"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(query)
    while (rs.next()) {
      queue += rs.getInt(1)
    }
    rs.close()
    stmt.close()
    queue
  }

  def getStartTaskKeyNum: Int = {
    val query = s"select ifnull(max(keyNum),0) from TaskOracleToCassandraCheckpoint where taskId = $taskId "
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(query)
    rs.next()
    val maxKeyNum = rs.getInt(1)
    rs.close()
    stmt.close()
    maxKeyNum
  }

  def main(args: Array[String]) {
    MetaDB.initTaskInfo(2)
    println(getStartTaskKeyNum)
  }

  def getTargetCassandraInfo: String = {
    val query = s"select keyspace from TargetCassandra where id = $targetId"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(query)
    rs.next()
    val data = rs.getString(1)
    rs.close
    stmt.close()
    data
  }

  def getSourceMax: Int = {
    val url = s"jdbc:oracle:thin:@${MetaDB.sourceConnString}"
    val props = new Properties();
    props.put("oracle.jdbc.ReadTimeout", "6000");
    props.put("user", MetaDB.sourceUsername);
    props.put("password", MetaDB.sourcePassword);
    val connSelf = DriverManager.getConnection(url, props)
    val rs = connSelf.createStatement().executeQuery("select max(id) from TBLDOWNUPDATELOG")
    rs.next()
    val maxNumber = rs.getLong(1)
    connSelf.close()
    (maxNumber / 5000).toInt
  }
}
