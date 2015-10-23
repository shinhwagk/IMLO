package org.gk.imlo.target

import akka.actor._
import org.gk.imlo.Message.{ImportRowToCassandraSuccess, TbllogRow}


class TargetSlaveCassandra extends Actor with ActorLogging {

  import com.datastax.driver.core.Cluster

  val cluster = Cluster.builder().addContactPoint("192.168.12.40").build();
  val session = cluster.connect("andrstore")
  session.getCluster.getMetadata.getKeyspace("aaa").getTable("aaa")
  val cql = "insert into tbllog " +
    "(TID,UUID,IMSI,BRAND,MODEL,CHANNEL,PLAT,ANDROIDVER,SCREENSIZE,LANG,APPSTOREVER,PROVIDER,CONNECTIONMODE,GETLOCTYPE,LOCSTR,COUNTRY,PROVINCE,CITY,IPADDR,ACCESSTYPE,CURRPAGE,PROPAGE,PROCONTENT,APPID,OTHERPARAS,CREATED,PHONE,PRODUCT,SDK,DISPLAY,CODENAME,TCARDSIZE,RAM,CPUCLOCKSPEED,SOURCE,SMSCENTER,ENC,PVER,IMEI,PKG)" +
    "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  val prepared = session.prepare(cql)

  override def receive: Receive = {
    case row: TbllogRow =>
      val bound_stmt = prepared.bind()
      bound_stmt.setLong("TID", row.TID)
      bound_stmt.setString("UUID", if (row.UUID == null) "null" else row.UUID)
      bound_stmt.setString("IMSI", row.IMSI)
      bound_stmt.setString("BRAND", row.BRAND)
      bound_stmt.setString("MODEL", row.MODEL)
      bound_stmt.setString("CHANNEL", row.CHANNEL)
      bound_stmt.setString("PLAT", row.PLAT)
      bound_stmt.setString("ANDROIDVER", row.ANDROIDVER)
      bound_stmt.setString("SCREENSIZE", row.SCREENSIZE)
      bound_stmt.setString("LANG", row.LANG)
      bound_stmt.setString("APPSTOREVER", row.APPSTOREVER)
      bound_stmt.setString("PROVIDER", row.PROVIDER)
      bound_stmt.setString("CONNECTIONMODE", row.CONNECTIONMODE)
      bound_stmt.setString("GETLOCTYPE", row.GETLOCTYPE)
      bound_stmt.setString("LOCSTR", row.LOCSTR)
      bound_stmt.setString("COUNTRY", row.COUNTRY)
      bound_stmt.setString("PROVINCE", row.PROVINCE)
      bound_stmt.setString("CITY", row.CITY)
      bound_stmt.setString("IPADDR", row.IPADDR)
      bound_stmt.setString("ACCESSTYPE", if (row.ACCESSTYPE == null) "null" else row.ACCESSTYPE)
      bound_stmt.setString("CURRPAGE", row.CURRPAGE)
      bound_stmt.setString("PROPAGE", row.PROPAGE)
      bound_stmt.setString("PROCONTENT", row.PROCONTENT)
      bound_stmt.setString("APPID", row.APPID)
      bound_stmt.setString("OTHERPARAS", row.OTHERPARAS)
      bound_stmt.setDate("CREATED", row.CREATED)
      bound_stmt.setString("PHONE", row.PHONE)
      bound_stmt.setString("PRODUCT", row.PRODUCT)
      bound_stmt.setString("SDK", row.SDK)
      bound_stmt.setString("DISPLAY", row.DISPLAY)
      bound_stmt.setString("CODENAME", row.CODENAME)
      bound_stmt.setLong("TCARDSIZE", row.TCARDSIZE)
      bound_stmt.setString("RAM", row.RAM)
      bound_stmt.setString("CPUCLOCKSPEED", row.CPUCLOCKSPEED)
      bound_stmt.setString("SOURCE", row.SOURCE)
      bound_stmt.setString("SMSCENTER", row.SMSCENTER)
      bound_stmt.setString("ENC", row.ENC)
      bound_stmt.setLong("PVER", row.PVER)
      bound_stmt.setString("IMEI", row.IMEI)
      bound_stmt.setString("PKG", row.PKG)
      val rsf = session.executeAsync(bound_stmt)
//      rs
//      //      val send = sender()
            val tid = row.TID
//      //      Future {
//      var time = 1
//      while (!rsf.isDone) {
//        Thread.sleep(time)
//        time += 10
//      }
      sender() ! ImportRowToCassandraSuccess(rsf,tid)
    //      } onSuccess { case a => send ! ImportRowToCassandraSuccess(tid) }
  }
}




