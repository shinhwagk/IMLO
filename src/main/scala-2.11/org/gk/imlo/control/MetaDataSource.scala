package org.gk.imlo.control

import java.sql.DriverManager
import java.util.Properties

import scala.collection.mutable

/**
 * Created by gk on 2015/10/13.
 */
class MetaDataSource {
  Class.forName("oracle.jdbc.driver.OracleDriver");
  private val queue = new mutable.Queue[java.sql.Connection]
  private val url = s"jdbc:oracle:thin:@192.168.20.248:1521/test";
  private val props = new Properties();
  props.put("oracle.jdbc.ReadTimeout", "6000");
  props.put("user", "goku");
  props.put("password", "goku");

  val conn = DriverManager.getConnection(url, props)

  val dataUrlConnect = {
    val url = s"jdbc:oracle:thin:@218.202.225.211:1521/sh11";
    val props = new Properties();
    props.put("oracle.jdbc.ReadTimeout", "6000");
    props.put("user", "andrstore");
    props.put("password", "andrstore");
    DriverManager.getConnection(url, props)
  }

  lazy val maxTid = {
    val conn2 = dataUrlConnect
    val rs = conn2.prepareStatement("select max(tid) from tbllog").executeQuery()
    rs.next()
    val tid = rs.getLong(1)
    conn2.close()
    tid - (tid % 5000)
  }

  def getATid: Option[Long] = {

    conn.setAutoCommit(false)
    val stmt1 = conn.createStatement()
    val rs = stmt1.executeQuery("select atid from importc where executing = 0 and rownum= 1")
    rs.next()
    val atid = if (rs.getRow != 0) Some(rs.getLong(1)) else None
    rs.close()
    stmt1.close()

    if (atid != None) {
      val stmt2 = conn.prepareStatement("update importc set EXECUTING=1 where atid = ?")
      stmt2.setLong(1, atid.get)
      stmt2.execute()
      stmt2.close
    }
    conn.commit()
    atid
  }

  def RecordAtidCount(aTid: Long, count: Int) = {
    val stmt = conn.prepareStatement("update importc set count=? where atid = ?")
    stmt.setInt(1, count)
    stmt.setLong(2, aTid)
    stmt.execute()
    stmt.close()

  }

  def getCount(aTid: Long) = {
    val stmt = conn.prepareStatement("select count from importc where atid = ?")
    stmt.setLong(1, aTid)
    val rs = stmt.executeQuery()
    rs.next()
    val count = rs.getInt(1)

    rs.close()
    stmt.close
    count
  }

  def clearATid(aTid: Long) = {
    val stmt = conn.prepareStatement("delete importc where atid = ? and executing=1")
    stmt.setLong(1, aTid)
    stmt.execute()
    stmt.close()
  }

  def restartAtid = {
    val stmt = conn.createStatement()
    stmt.execute("update importc set executing = 0")
    stmt.close
  }

}