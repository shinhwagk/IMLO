package org.gk.imlo.source

import akka.actor._


class SourceSlaveOracle extends Actor with ActorLogging {

//  val stmt = sourceExecInfo.getPrepareStatement

  override def preRestart(reason: Throwable, message: Option[Any]) {
    println("actor:" + self.path + ",preRestart child, reason:" + reason + ", message:" + message)
    println("重新发送至:" + self.path + ". 父:" + sender().path)
  }


  override def receive: Receive = {
    case tid: Long =>
//      stmt.setLong(1, tid)
//      stmt.setLong(2, tid + 5000)
//      val rs = stmt.executeQuery()
    //      val rs = stmt.executeQuery();
    //
    //      while(rs.next()){
    //        colInfo.foreach{p=>
    //          p._2 match {
    //            case "VARCHAR2" =>
    //              rs.getString(p._1)
    //            case "NUMBER" =>
    //              rs.getLong(p._1)
    //            case "DATE" =>
    //              rs.getTimestamp(p._1)
    //          }
    //                  }
    //      }
    //      try {
    //        while (rs.next()) {
    //          val TID = rs.getLong("TID")
    //          val IMSI = rs.getString("IMSI")
    //          val UUID = rs.getString("UUID")
    //          val BRAND = rs.getString("BRAND")
    //          val MODEL = rs.getString("MODEL")
    //          val CHANNEL = rs.getString("CHANNEL")
    //          val PLAT = rs.getString("PLAT")
    //          val ANDROIDVER = rs.getString("ANDROIDVER")
    //          val SCREENSIZE = rs.getString("SCREENSIZE")
    //          val LANG = rs.getString("LANG")
    //          val APPSTOREVER = rs.getString("APPSTOREVER")
    //          val PROVIDER = rs.getString("PROVIDER")
    //          val CONNECTIONMODE = rs.getString("CONNECTIONMODE")
    //          val GETLOCTYPE = rs.getString("GETLOCTYPE")
    //          val LOCSTR = rs.getString("LOCSTR")
    //          val COUNTRY = rs.getString("COUNTRY")
    //          val PROVINCE = rs.getString("PROVINCE")
    //          val CITY = rs.getString("CITY")
    //          val IPADDR = rs.getString("IPADDR")
    //          val ACCESSTYPE = rs.getString("ACCESSTYPE")
    //          val CURRPAGE = rs.getString("CURRPAGE")
    //          val PROPAGE = rs.getString("PROPAGE")
    //          val PROCONTENT = rs.getString("PROCONTENT")
    //          val APPID = rs.getString("APPID")
    //          val OTHERPARAS = rs.getString("OTHERPARAS")
    //          val CREATED = rs.getTimestamp("CREATED")
    //          val PHONE = rs.getString("PHONE")
    //          val PRODUCT = rs.getString("PRODUCT")
    //          val SDK = rs.getString("SDK")
    //          val DISPLAY = rs.getString("DISPLAY")
    //          val CODENAME = rs.getString("CODENAME")
    //          val TCARDSIZE = rs.getLong("TCARDSIZE")
    //          val RAM = rs.getString("RAM")
    //          val CPUCLOCKSPEED = rs.getString("CPUCLOCKSPEED")
    //          val SOURCE = rs.getString("SOURCE")
    //          val SMSCENTER = rs.getString("SMSCENTER")
    //          val ENC = rs.getString("ENC")
    //          val PVER = rs.getLong("PVER")
    //          val IMEI = rs.getString("IMEI")
    //          val PKG = rs.getString("PKG")
    //          rowSet += TbllogRow(TID, UUID, IMSI, BRAND, MODEL, CHANNEL, PLAT, ANDROIDVER, SCREENSIZE, LANG, APPSTOREVER, PROVIDER, CONNECTIONMODE, GETLOCTYPE, LOCSTR, COUNTRY, PROVINCE, CITY, IPADDR, ACCESSTYPE, CURRPAGE, PROPAGE, PROCONTENT, APPID, OTHERPARAS, CREATED, PHONE, PRODUCT, SDK, DISPLAY, CODENAME, TCARDSIZE, RAM, CPUCLOCKSPEED, SOURCE, SMSCENTER, ENC, PVER, IMEI, PKG)
    //        }
    //
    //        rs.close()
    //        stmt.close()
    //
    //        if (rowSet.size > 0) {
    //          metaData.RecordAtidCount(tid, rowSet.size)
    //          rowSet.foreach(p => {
    //            sender() ! p
    //          })
    //        } else {
    //          metaData.clearATid(tid)
    //        }
    //
    //      } catch {
    //        case ex: Exception => {
    //          fff ! tid
    //          println(ex.getMessage)
    //        }
    //      } finally {
    //        //        ocp.pushConn(conn)
    //      }
  }


  //  def typeInfo: Set[Map[String, String]] = {
  //    val row = conn.prepareStatement("SELECT to_char(created,'yyyy') year,to_char(created,'mm') month,to_char(created,'dd') day,to_char(created,'hh24') hour,to_char(created,'mi') minute,to_char(created,'ss') second from tbllog where rownum<1000").executeQuery()
  //    val colNum = row.getMetaData.getColumnCount
  //    for (i <- 1 to colNum) {
  //      val colType = row.getMetaData.getColumnTypeName(i)
  //      val colName = row.getMetaData.getColumnName(i)
  //      mmm += Map(colName -> colType)
  //
  //    }
  //    mmm
  //  }
}






