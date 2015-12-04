package test

import java.sql.DriverManager
import java.util.{Date, Properties}

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{ConsistencyLevel, ResultSetFuture, Session}
import org.gk.imlo.Message.RowsInfo

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by gk on 2015/11/10.
  */

trait TaskTrait {
  def pullRowSet(KeyNum: Int): RowsInfo

  def pushRowSet(ip: String, rowsInfo: RowsInfo, session: Session): Long
}

class Task extends TaskTrait {

  override def pullRowSet(KeyNum: Int): RowsInfo = {
    println("开始从db获取数据", KeyNum)
    val url = s"jdbc:oracle:thin:@${MetaDB.sourceConnString}"
    val sql = MetaDB.sql
    val step = MetaDB.step
    val props = new Properties();
    props.put("oracle.jdbc.ReadTimeout", "6000");
    props.put("oracle.net.CONNECT_TIMEOUT", "6000");
    props.put("user", MetaDB.sourceUsername);
    props.put("password", MetaDB.sourcePassword);
    val conn = DriverManager.getConnection(url, props)

    val keyNum = (KeyNum * 5000).toLong
    println(conn, KeyNum)
    val stmt = conn.prepareStatement(sql)
    stmt.setFetchSize(Array(1, 1000, 10000, 10000, 10000, 10000, 10000)(Random.nextInt(7)))
    stmt.setLong(1, keyNum)
    stmt.setLong(2, keyNum + step.toLong)
    val rs = stmt.executeQuery()
    val colNum = rs.getMetaData.getColumnCount
    val rows = ArrayBuffer[Array[(String, String, Any)]]()
    println("开始处理", KeyNum)
    while (rs.next()) {
      val row = new Array[(String, String, Any)](colNum)
      //数组从0开始，但是jdbc数据从1开始.
      for (i <- 0 to colNum - 1) {
        val colType = rs.getMetaData.getColumnTypeName(i + 1)
        val colName = rs.getMetaData.getColumnName(i + 1)
        colType match {
          case "NUMBER" =>
            row(i) = ("Long", colName, rs.getLong(i + 1))
          case "CHAR" =>
            row(i) = ("String", colName, rs.getString(i + 1))
          case "VARCHAR2" =>
            row(i) = ("String", colName, rs.getString(i + 1))
          case "DATE" =>
            row(i) = ("Timestamp", colName, rs.getTimestamp(i + 1))
        }
      }
      rows += row
    }

    println((keyNum / 5000).toInt, "执行完毕", rows.size)
    rs.close()
    stmt.close()
    conn.close()
    RowsInfo(keyNum.toLong, rows)
  }

  override def pushRowSet(ip: String, rowsInfo: RowsInfo, session: Session): Long = {
    println(rowsInfo.key, "准备插入cassandra")
    val startTime = System.currentTimeMillis()
    val rsetBuffer = new ArrayBuffer[ResultSetFuture]()
    for (r <- rowsInfo.rows) {
      QueryBuilder.insertInto()
      val ps = session.prepare(MetaDB.cql)
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
    for (i <- rsetBuffer) {
      i.getUninterruptibly.getExecutionInfo
    }

    println(rowsInfo.key / 5000, rowsInfo.rows.size, (System.currentTimeMillis() - startTime), "每秒插入数量估计...", ip)
    rowsInfo.key
  }
}