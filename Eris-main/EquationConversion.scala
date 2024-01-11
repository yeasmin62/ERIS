import scalafx.Includes._
import scalafx.scene.layout._
import scalafx.scene.control.{Button, Tab, TabPane, Label}
import scalafx.geometry.HPos
import scalafx.geometry.Pos.Center
import java.util.regex.Pattern
class EquationConversion {

    def convertExpression(expression: String): String = {
      val updatedExpression = expression
        .replaceAll("Plus", "")
        .replaceAll(",", " + ")

      val constraints = ConstraintsConvert(updatedExpression)
      constraints
    }
    def button(goToScene1: () => Unit) : Button ={
      val home = new Button("HOME")
      home.alignment = Center
      home.setOnAction(_ => goToScene1())
      home
    }

    def LabelCompleted(): (Label,Label) = {
      val completed = new Label("Completed!")
      {
        visible = false
      }
      val loadingLabel = new Label("Loading....")
      {
        visible = false
      }

      (loadingLabel,completed)
    }


    def queryconvert(q:String):String = {
      val convertedq = q.toString().replaceAll("\\(","\n\\(")
      convertedq
    }

  def transformTableLast(lastPart: String): String = {
  val rm = List("row", "array", "::", "term", "sparsevec", "[]")
  val rm1 = List("\\(\\)")

  // Function to remove items from the list, properly escaping special characters
  def removeItems(str: String, items: List[String]): String = {
    items.foldLeft(str)((acc, item) => acc.replaceAll(Pattern.quote(item), ""))
  }

  // Remove items from the string
  val result = removeItems(lastPart, rm)
  result
}

  def ConstraintsConvert(lastPart: String): String = {
    val rm = List("Num", "1.0", "Var")
    val rm1 = List("() *", "))", "((", "( (")
// Function to remove items from the list
    def removeItems(str: String, items: List[String]): String = {
      items.foldLeft(str)((acc, item) => acc.replace(item, ""))
    }
// Remove items from the string
    val result = removeItems(lastPart, rm)
    val result1 = removeItems(result, rm1)

    result1
  }
}
