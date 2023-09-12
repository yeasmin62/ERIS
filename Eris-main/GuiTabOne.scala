// import scala.util.control.Breaks._
// import com.sun.glass.ui.Application
// import javafx.scene.control.ScrollPane
// import scalafx.Includes._
// import scalafx.application
// import scalafx.application.{AppHelper, JFXApp}
// import scalafx.scene.Scene
// import scalafx.scene.control._
// import scalafx.event.ActionEvent
// import scalafx.geometry.Orientation
// import scalafx.geometry.Orientation.Horizontal
// import scalafx.geometry.Pos.Center
// import scalafx.scene.layout.Priority.Always
// import scalafx.scene.layout.TilePane.getAlignment
// import scalafx.scene.layout._
// import scalafx.stage.{FileChooser, Window}
// import scalafx.stage.FileChooser
// import scala.collection.mutable.ListBuffer
// import scalafx.concurrent.Task
// import scala.io.Source
// import scalafx.geometry.HPos
// import java.awt.Insets
// import scalafx.geometry
// import scala.collection.mutable.HashMap
// import scalafx.scene.text.FontWeight
// import scala.io.Source
// object GuiTabOne{
//     def tabone(): VBox = {
//         var scene1: Scene = null
//   var scene2: Scene = null
//   var input_text: String = ""
//   var flagV: String = ""
//   var enc: String = ""
//   var flag_null: String = ""
//   var flag_error: String = ""
//   var encoding: Encoding = _

//   val label = new Label("Which Encoding would you prefer?")
//     val rbn1 = new RadioButton("NF2_SparseV") {
//       onAction = (e: ActionEvent) => {
//         enc = "nf2_sparsev"
//         encoding = Encoding.encoder_to_use(enc)
//       }
//     }
//     val rbn2 = new RadioButton("Partitioning") {
//       onAction = (e: ActionEvent) => {
//         enc = "partitioning"
//         encoding = Encoding.encoder_to_use(enc)
//       }
//     }

//     val toggol1 = new ToggleGroup
//     toggol1.toggles = List(rbn1, rbn2)

//     val label1 = new Label("Variable Generation") {}
//     val rbn3 = new RadioButton("V+X") {
//       onAction = (e: ActionEvent) => {
//         flagV = "true"
//       }
//     }
//     val rbn4 = new RadioButton("V*(1+X)") {
//       onAction = (e: ActionEvent) => {
//         flagV = "false"
//       }
//     }
//     val toggol2 = new ToggleGroup
//     toggol2.toggles = List(rbn3, rbn4)
//     val label2 = new Label("Do you want to consider NULL in cost function?") {}
//     val rbn_null = new RadioButton("Yes") {
//       onAction = (e: ActionEvent) => {
//         flag_null = "true"
//       }
//     }
//     val rbn_not_null = new RadioButton("No") {
//       onAction = (e: ActionEvent) => {
//         flag_null = "false"
//       }
//     }
//     val toggol3 = new ToggleGroup
//     toggol3.toggles = List(rbn_null, rbn_not_null)
//     val label3 = new Label("Cost functions") {}

//     val rbn_s_error = new RadioButton("Average Square Error (ASE)") {
//       onAction = (e: ActionEvent) => {
//         flag_error = "ASE"
//         print(flag_error)
//       }
//     }
//     val rbn_a_error = new RadioButton("Average Absolute Error (AAE)") {
//       onAction = (e: ActionEvent) => {
//         flag_error = "AAE"
//         print(flag_error)
//       }
//     }
//     val rbn_value_interval = new RadioButton("Error with Variable Constraints"){
//       onAction = (e: ActionEvent) => {
//         flag_error = "Value_Interval"
//         print(flag_error)
//       }
//     }
//     val toggol4 = new ToggleGroup
//     toggol4.toggles = List(rbn_s_error, rbn_a_error,rbn_value_interval)

//     ////// Loading Tasks ///////
//     def createLoadingTask(): Task[Unit] = Task {
//       // Simulating a time-consuming process
//       for ((k, v) <- ctx1) {
//         // print(k)
//         Loader.load(
//           connector,
//           k.toLowerCase(),
//           encoding,
//           false,
//           flagV.toBoolean
//         )
//       }
//       Thread.sleep(10000)
//     }


//     val progressBar = new ProgressBar {
//       // value_= (0) // set initial value to 0
//       prefWidth = 200
//       progress = 0
//     }


//     val loadingLabel = new Label("Loading...") {
//       visible = false
//     }
//     val completed = new Label("Completed!!") {
//       visible = false
//     }
//     val tab1hbox = new HBox {
//       spacing = 20
//       children = Seq(loadingLabel, completed)
//     }

//     val loadbtn = new Button("Load") {
//       onAction = (e: ActionEvent) => {
//         var alert = new GuiAlert()
//         (enc, flagV) match {
//           case ("", _) => alert.showWarning("Please select encoding")
//           case (_, "") => alert.showWarning("Please select variable")
//           case (_, _) => {
//             val loadingTask = createLoadingTask()

//             loadingTask.setOnRunning { _ =>
//               progressBar.progress = -1 // Indeterminate progress
//               completed.visible = false
//               loadingLabel.visible = true

//             }

//             loadingTask.setOnSucceeded { _ =>
//               progressBar.progress = 1 // Completed progress
//               // loadingLabel.text = "Completed!!"
//               loadingLabel.visible = false
//               completed.visible = true
//               // tab2.disable = false
//               tabpane.selectionModel().select(tab2)
//             }

//             new Thread(loadingTask).start()
//           }
//         }
      

//       }
//     }

//     loadbtn.alignment = Center

//     var tab1vbox = new VBox() {
//       padding = geometry.Insets(0, 0, 0, 80)
//       // spacing = 10
//     }

//     // tab1vbox.alignment = Center
//     tab1vbox.children = List(
//       new Pane { prefHeight = 30 },
//       label,
//       rbn1,
//       rbn2,
//       new Pane { prefHeight = 10 },
//       label1,
//       rbn3,
//       rbn4,
//       new Pane { prefHeight = 10 },
//       label2,
//       rbn_null,
//       rbn_not_null,
//       new Pane { prefHeight = 10 },
//       label3,
//       rbn_s_error,
//       rbn_a_error,
//       rbn_value_interval,
//       new Pane { prefHeight = 10 },
//       progressBar,
//       tab1hbox,
//       loadbtn
//     )

//     tab1vbox
//     }
// }