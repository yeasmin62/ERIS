import scalafx.application.JFXApp
import scalafx.Includes._
import scalafx.geometry.Pos
import scalafx.scene.Scene
import scalafx.scene.control.Alert.AlertType
import scalafx.scene.control.{Alert, Button, ProgressBar}
import scalafx.scene.layout.{BorderPane, HBox}
import scalafx.scene.control.{Alert, ButtonType, Button, Label}
import scala.collection.mutable.ListBuffer

class GuiAlert extends JFXApp {
  // Create an alert for the first scene
  def showalert(): ButtonType = {
    val alert = new Alert(AlertType.Information) {
      title = "Alert"
      headerText = "This is an alert!"
      contentText = "Do you want to continue?"
      buttonTypes = Seq(ButtonType.Yes, ButtonType.No)
    }

    alert.showAndWait().getOrElse(ButtonType.No)
  }

  

  def showWarning(msg: String): Unit = {
    val alert = new Alert(Alert.AlertType.Warning)
    alert.setTitle("Warning")
    alert.setHeaderText("Warning Message")
    alert.setContentText(msg)

    val result = alert.showAndWait()
    result.foreach(_ => alert.close())
  }

  def showOk(msg: String): Unit = {
    val alert = new Alert(Alert.AlertType.Information)
    alert.setTitle("Notification")
    alert.setHeaderText("Correct Insertion")
    alert.setContentText(msg)

    val result = alert.showAndWait()
    result.foreach(_ => alert.close())
  }

  def showprogress(): (ProgressBar, Label, Label) = {
    val progressBar = new ProgressBar {
      prefWidth = 200
    }

    val loadingLabel = new Label("Loading...") {
      visible = false
    }
    val completed = new Label("Completed!!") {
      visible = false
    }

    (progressBar, loadingLabel, completed)
  }

  def parseBound(input: String): Double = input.toLowerCase match {
    case "infinity"  => Double.PositiveInfinity
    case "-infinity" => Double.NegativeInfinity
    case _ =>
      try input.toDouble
      catch {
        case _: NumberFormatException => Double.NaN
      }
  }
  def processBounds(
      input1: String,
      input2: String,
      boundlist: ListBuffer[Double]
  ): (ListBuffer[Double], Boolean) = {
    val value1 = parseBound(input1)
    val value2 = parseBound(input2)
    if (value1.isNaN || value2.isNaN) {
      showWarning("Error: One or both values are not valid numbers.")
      (boundlist, false)
    } else {
      (value1, value2) match {
        case (Double.PositiveInfinity, _) =>
          showWarning("Error: Lower Bound cannot be positive infinity.")
          return (boundlist, false)

        case (_, Double.NegativeInfinity) =>
          showWarning("Error: Upper Bound cannot be negative infinity.")
          return (boundlist, false)

        case (v1, v2) if v1 > v2 =>
          showWarning("Error: Lower Bound cannot be greater than Upper Bound.")
          return (boundlist, false)

        case (v1, v2) if v2 < v1 =>
          showWarning("Error: Upper Bound cannot be smaller than Lower Bound.")
          return (boundlist, false)

        case (v1, v2) =>
          boundlist.clear()
          boundlist += value1
          boundlist += value2
          showOk("Bounds Inserted Correctly, you can continue")
          return (boundlist, true)
      }
    }
  }
}

//     val button = new Button("Go to Second Scene")
//     button.onAction = _ => stage.scene = secondScene

//     val layout = new BorderPane {
//       center = button
//       alignment = Pos.Center
//       style = "-fx-padding: 20px"
//     }

//     root = layout
//   }

//   // Create the second scene
//   val secondScene = new Scene {
//     val label = new Button("Second Scene")
//     label.onAction = _ => stage.scene = firstScene

//     val layout = new BorderPane {
//       center = label
//       alignment = Pos.Center
//       style = "-fx-padding: 20px"
//     }

//     root = layout
//   }

//   // Set the first scene as the initial scene
//   stage = new JFXApp.PrimaryStage {
//     title = "Scene Transition App"
//     scene = firstScene
//   }
// }
