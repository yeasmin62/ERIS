import scala.util.control.Breaks._
import com.sun.glass.ui.Application
import javafx.scene.control.ScrollPane
import scalafx.Includes._
import scalafx.application
import scalafx.application.{AppHelper, JFXApp}
import scalafx.scene.Scene
import scalafx.scene.control._
import scalafx.event.ActionEvent
import scalafx.geometry.Orientation
import scalafx.geometry.Orientation.Horizontal
import scalafx.geometry.Pos.Center
import scalafx.scene.layout.Priority.Always
import scalafx.scene.layout.TilePane.getAlignment
import scalafx.scene.layout._
import scalafx.stage.{FileChooser, Window}
import scalafx.stage.FileChooser
import scala.collection.mutable.ListBuffer
import scalafx.concurrent.Task
import scala.io.Source
import scalafx.geometry.HPos
import java.awt.Insets
import scalafx.geometry
import scala.collection.mutable.HashMap
import scalafx.scene.text.FontWeight
import scala.io.Source

object GuiTabFour {
  def tabfour(): ScrollPane = {
    val FAQOne = new Label("FAQ1: How the system works?"){
        style = "-fx-font-weight: bold;"
    }
    val FAQOneAns = new Label(
      "1. If you are running the GUI for the first time, Follow the instructions below: \n")
    val FAQOneAns1 = new Label(
      "2. Select the options that you want to test for your data domain and press the Load button\n This will create tableviews in the database according to the selected option.\n")
    val FAQOneAns2 = new Label("Once the loading is completed, you will proceed to second tab.\n")
    val FAQOneAns3 = new Label("3. Here, you can select any field to make it ground truth and update the schema.\n After updating, yo need to load again with Refresh Load button.\n Once the loding is completed, you will procedd ot third tab.")
    val FAQOneAns4 = new Label("4. In third tab, you need to choose the file of your query which will be shown in the textarea and you can modify from there and press Run. \n After completing running, you will see the results.")
    val FAQTwo = new Label("FAQ2: Which operations can I apply?"){
        style = "-fx-font-weight: bold;"
    }
    val FAQTwoAns = new Label(
      "Operations Included: Select,\n Project, \n ProjectAway, \n Join, \n Renaming, \n Difference, \n Aggregation, \n" +
        "UNION, \n DUNION, \n Coalescing. \n")
    val tab4vbox = new VBox() {
      spacing = 10
      padding = geometry.Insets(0, 0, 0, 20)
      // alignment = Center
      children = Seq(FAQOne, FAQOneAns, FAQOneAns1, FAQOneAns2, FAQOneAns3, FAQOneAns4, FAQTwo, FAQTwoAns)
    }

    val tab4scroll = new ScrollPane()
    tab4scroll.content = tab4vbox
    tab4scroll
  }
}
