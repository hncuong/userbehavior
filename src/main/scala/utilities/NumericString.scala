package utilities

/**
  * Created by cuonghn on 7/20/16.
  */
object NumericString {
  def isNumeric(input: String): Boolean = {
    var isNumeric = false
    if (input.length > 0){
      isNumeric = input.forall(_.isDigit)
    }
    return isNumeric
  }
}
