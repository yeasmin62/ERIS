import scalafx.Includes._
import scalafx.scene.layout._
import scalafx.scene.control.{Button, Tab, TabPane, Label}
import scalafx.geometry.HPos
import scalafx.geometry.Pos.Center
class EquationConversion {

    def convertExpression(expression: String): String = {
      val updatedExpression = expression
        .replaceAll("Plus", "")
        .replaceAll(",", " + ")

      updatedExpression
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
}
