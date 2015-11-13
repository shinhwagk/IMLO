package test.target

import java.util.Date

import com.datastax.driver.core.{ConsistencyLevel, ResultSetFuture, Cluster, PoolingOptions}
import org.gk.imlo.Message.RowsInfo

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gk on 2015/11/10.
  */
class Cassandra {
  //  val poolingOptions = new PoolingOptions()
  //  poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 10, 50).setConnectionsPerHost(HostDistance.REMOTE, 10, 50)
  val cluster = Cluster.builder().addContactPoints("192.168.12.41").build();
  //  val cql = "insert into TBLDOWNUPDATELOG " +
  //    "(ID,SERIALNO,HW,HWV,SWV,PREVERSION,CURVERSION,UPDATESTATUS,CREATEDATE,BDT,CUST,KERNEL,MAF,BOARD,LANG,NET,OPRATOR,SMSC,LOGTYPE,AID,IMSI)" +
  //    "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  //  val cql = "insert into TBLLOG (TID,UUID,IMSI,BRAND,MODEL,CHANNEL,PLAT,ANDROIDVER,SCREENSIZE,LANG,APPSTOREVER,PROVIDER,CONNECTIONMODE,GETLOCTYPE,LOCSTR,COUNTRY,PROVINCE,CITY,IPADDR,ACCESSTYPE,CURRPAGE,PROPAGE,PROCONTENT,APPID,OTHERPARAS,CREATED,PHONE,PRODUCT,SDK,DISPLAY,CODENAME,TCARDSIZE,RAM,CPUCLOCKSPEED,SOURCE,SMSCENTER,ENC,PVER,IMEI,PKG) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  val cql = "insert into tbllog (UUID,area,IMSI,BRAND,MODEL,CHANNEL,PLAT,ANDROIDVER,SCREENSIZE,LANG,APPSTOREVER,PROVIDER,CONNECTIONMODE,GETLOCTYPE,LOCSTR,COUNTRY,PROVINCE,CITY,IPADDR,ACCESSTYPE,CURRPAGE,PROPAGE,PROCONTENT,APPID,OTHERPARAS,yyyy,mm,dd,hh24,mi,ss,PHONE,PRODUCT,SDK,DISPLAY,CODENAME,TCARDSIZE,RAM,CPUCLOCKSPEED,SOURCE,SMSCENTER,ENC,PVER,IMEI,PKG) values(now(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  val session = cluster.connect("bigdataanalysis")

  def exec(rowsInfo: RowsInfo) = {
    println(rowsInfo.key, "准备插入cassandra")
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
      i.getUninterruptibly.getExecutionInfo
    }

    println(rowsInfo.key, rowsInfo.rows.size, (System.currentTimeMillis() - startTime) / 1000, "每秒插入数量估计...")
    rowsInfo.key
  }
}
