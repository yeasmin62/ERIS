import scalafx.application.JFXApp
import scalafx.Includes._
import scalafx.geometry.Pos
import scalafx.scene.Scene
import scalafx.scene.control.Alert.AlertType
import scalafx.scene.control.{Alert, Button,ProgressBar}
import scalafx.scene.layout.{BorderPane, HBox}
import scalafx.scene.control.{Alert, ButtonType, Button, Label}

class GuiAlert extends JFXApp {
  // Create an alert for the first scene
  def showalert(): ButtonType = {
    val alert = new Alert(AlertType.Information) {
      title = "Alert"
      headerText = "This is an alert!"
      contentText = "Do you want to continue?"
      buttonTypes = Seq(ButtonType.Yes,ButtonType.No)
    }

    alert.showAndWait().getOrElse(ButtonType.No)
  }


def showWarning(msg:String): Unit = {
    val alert = new Alert(Alert.AlertType.Warning)
    alert.setTitle("Warning")
    alert.setHeaderText("Warning Message")
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
    val completed = new Label("Completed!!")
    {
      visible = false
    }

    (progressBar, loadingLabel, completed)
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