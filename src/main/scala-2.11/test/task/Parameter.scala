package test.task

import test.meta.MetaDB

/**
  * Created by gk on 2015/11/13.
  */
class Parameter(taskId: Int) {
  val taskInfo = MetaDB.initTaskInfo(taskId)


}
