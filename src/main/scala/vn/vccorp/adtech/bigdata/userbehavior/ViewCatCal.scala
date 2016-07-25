package vn.vccorp.adtech.bigdata.userbehavior

/**
  * Created by cuonghn on 7/25/16.
  */
object ViewCatCal {
  def checkView( paidItem : Int, viewId : Int): Int ={
    val diffId = viewId - paidItem
    if (diffId % MuachungPathParse.itemIdMaxRange == 0 ){
      return
    }
    //if (diffId >= 0 && diffId <= MuachungPathParse.idPaidMinRange)
  }
}
