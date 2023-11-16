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

object GuiNew extends JFXApp {
  var scene1: Scene = null
  var scene2: Scene = null
  var input_text: String = ""
  var flagV: String = ""
  var enc: String = ""
  var flag_null: String = ""
  var flag_error: String = ""
  var encoding: Encoding = _

  stage = new JFXApp.PrimaryStage {
    title.value = "Eris: Discord measurement prototype"
  }

  scene1 = new Scene(400, 450) {

    // var input_text: String = " "

    // reading the configuration file for user credentials i.e database info
    var userConfig = Array[String]()
    var connectionProfile = Source.fromFile("config.txt")
    for (line <- connectionProfile.getLines) {
      userConfig = userConfig :+ line.toString()
    }
    connectionProfile.close

    var connector =
      Connector(userConfig(0), userConfig(1), userConfig(2), userConfig(3))
    var conn = connector.getConnection()

    var st = conn.createStatement()
    var ctx = Database.loadSchema_for_gui(conn)
    var ctx1 = Database.loadSchema(conn) // keeps all the key values
    // print("ctx" + ctx + "\n")
    // print("ctx1" + ctx1)

    val tabpane = new TabPane()
    val tab1 = new Tab(){
      closable = false
      text = "General"
    }
    val tab2 = new Tab(){
      text = "Ground Truth"
      closable = false
      disable = true

    }
    val tab3 = new Tab(){
      text = "Query Input"
      closable = false
      disable = true
    }
    val tab4 = new Tab(){
      text = "Help"
      closable = false
    }


    ////////// Tab 1 /////////////
    val label = new Label("Which Encoding would you prefer?")
    val rbn1 = new RadioButton("NF2_SparseV") {
      onAction = (e: ActionEvent) => {
        enc = "nf2_sparsev"
        encoding = Encoding.encoder_to_use(enc)
      }
    }
    val rbn2 = new RadioButton("Partitioning") {
      onAction = (e: ActionEvent) => {
        enc = "partitioning"
        encoding = Encoding.encoder_to_use(enc)
      }
    }

    val toggol1 = new ToggleGroup
    toggol1.toggles = List(rbn1, rbn2)

    val label1 = new Label("Variable Generation") {}
    val rbn3 = new RadioButton("V+X") {
      onAction = (e: ActionEvent) => {
        flagV = "true"
      }
    }
    val rbn4 = new RadioButton("V*(1+X)") {
      onAction = (e: ActionEvent) => {
        flagV = "false"
      }
    }
    val toggol2 = new ToggleGroup
    toggol2.toggles = List(rbn3, rbn4)
    val label2 = new Label("Do you want to consider NULL in cost function?") {}
    val rbn_null = new RadioButton("Yes") {
      onAction = (e: ActionEvent) => {
        flag_null = "true"
      }
    }
    val rbn_not_null = new RadioButton("No") {
      onAction = (e: ActionEvent) => {
        flag_null = "false"
      }
    }
    val toggol3 = new ToggleGroup
    toggol3.toggles = List(rbn_null, rbn_not_null)
    val label3 = new Label("Cost functions") {}

    val rbn_s_error = new RadioButton("Average Square Error (ASE)") {
      onAction = (e: ActionEvent) => {
        flag_error = "ASE"
        print(flag_error)
      }
    }
    val rbn_a_error = new RadioButton("Average Absolute Error (AAE)") {
      onAction = (e: ActionEvent) => {
        flag_error = "AAE"
        print(flag_error)
      }
    }
    val rbn_value_interval = new RadioButton("Error with Variable Constraints"){
      onAction = (e: ActionEvent) => {
        flag_error = "Value_Interval"
        print(flag_error)
      }
    }
    val toggol4 = new ToggleGroup
    toggol4.toggles = List(rbn_s_error, rbn_a_error,rbn_value_interval)

    ////// Loading Tasks ///////
    def createLoadingTask(): Task[Unit] = Task {
      // Simulating a time-consuming process
      for ((k, v) <- ctx1) {
        // print(k)
        Loader.load(
          connector,
          k.toLowerCase(),
          encoding,
          false,
          flagV.toBoolean
        )
      }
      Thread.sleep(10000)
    }


    val progressBar = new ProgressBar {
      // value_= (0) // set initial value to 0
      prefWidth = 200
      progress = 0
    }


    val loadingLabel = new Label("Loading...") {
      visible = false
    }
    val completed = new Label("Completed!!") {
      visible = false
    }
    val tab1hbox = new HBox {
      spacing = 20
      children = Seq(loadingLabel, completed)
    }

    val next1 = new Button("Next")
    {
      onAction = (e: ActionEvent) => {
        var alert = new GuiAlert()
        (enc, flagV, flag_null, flag_error) match {
          case ("", _, _, _) => alert.showWarning("Please select encoding")
          case (_, "", _, _) => alert.showWarning("Please select variable")
          case (_, _, "", _) => alert.showWarning("Please select null option")
          case (_, _, _, "") => alert.showWarning("Please select cost")
          case (_, _, _, _) => {
            tab2.disable = false
            tabpane.selectionModel().select(tab2)

          }
          
        }
        
      }
    }

    val loadbtn = new Button("Load") {
      onAction = (e: ActionEvent) => {
        var alert = new GuiAlert()
        (enc, flagV) match {
          case ("", _) => alert.showWarning("Please select encoding")
          case (_, "") => alert.showWarning("Please select variable")
          case (_, _) => {
            val loadingTask = createLoadingTask()

            loadingTask.setOnRunning { _ =>
              progressBar2.progress = -1 // Indeterminate progress
              completed.visible = false
              loadingLabel.visible = true

            }

            loadingTask.setOnSucceeded { _ =>
              progressBar2.progress = 1 // Completed progress
              // loadingLabel.text = "Completed!!"
              loadingLabel.visible = false
              completed.visible = true
              tab3.disable = false
              tabpane.selectionModel().select(tab3)
            }

            new Thread(loadingTask).start()
          }
        }
      

      }
    }

    loadbtn.alignment = Center

    ////////// Tab 3 /////////////
    var InputArea = new TextArea
    InputArea.prefHeight = 200
    InputArea.prefWidth = 280
    InputArea.promptText =
      "Insert the query or select the spec file with choose file option"
    // InputArea.promptText = "Example"
    InputArea.focused.onChange {
      input_text = InputArea.text.apply
      // println(input_text)
    }

    ////////// Tab 2 /////////////
    // var tab2vbox = new VBox(){
    //   padding = geometry.Insets(0,0,0,30)
    //   spacing = 10
    //   alignment = Center
    // }
    // tab2vbox.children.add(new Pane{prefHeight = 10})
    var keylist: ListBuffer[String] = ListBuffer()
    var valuelist: ListBuffer[String] = ListBuffer()
    var deletekeylist: ListBuffer[String] = ListBuffer()
    var deletevaluelist: ListBuffer[String] = ListBuffer()
    var keyvalue: Map[String, String] = Map()
    var keyvarfree: List[String] = List()
    var valuevarfree: List[String] = List()

    // extracted values to create the tablename and attribute name

    for ((k, v) <- ctx) {

      val st1 = v.toString
      val l = st1.length

      if (!(st1(l - 1) == ';' && st1(l - 2) == ';')) {
        val k1 = st1.split(';')
        if (k1.length <= 2) {
          val c = k1.length
          for (i <- 1 to c - 1) {
            keyvalue += (k.toString -> k1(i))
          }
        } else {
          if (k1.length >= 3) {
            for (i <- 1 to k1.length - 2) {
              keyvalue += (k.toString -> k1(i))
            }
          }
        }
        if (k1.length >= 3) {
          val k2 = k1(k1.length - 1).split(',')
          for (j <- k2) {
            keyvarfree = keyvarfree :+ k.toString
            valuevarfree = valuevarfree :+ j
          }
        }

      }

    }
    // print(keyvalue)

    val selectedMapping: HashMap[String, String] = HashMap.empty
    val unselectedMapping: HashMap[String, String] = HashMap.empty

    val selectedCheckboxes: ListBuffer[CheckBox] = ListBuffer.empty

    val tab2vbox = new VBox {
      spacing = 10
      padding = geometry.Insets(0, 0, 0, 30)
      alignment = Center
    }
    tab2vbox.children.append(new Pane { prefHeight = 20 })

    for ((key, values) <- keyvalue) {
      val keyLabel = new Label(key)
      val checkboxes = values.split(",").map { value =>
        val checkbox = new CheckBox(value) {
          for (c <- 0 to keyvarfree.length - 1) {
            if (key == keyvarfree(c) && value == valuevarfree(c)) {
              selected = true
              // selectedMapping += (key -> (selectedMapping.getOrElse(key, "") + checkbox.text()))
            }
          }
        }        
        checkbox
      }
      checkboxes.foreach { checkbox =>
        checkbox.selected.onChange { (_, oldValue, newValue) =>
          val keyValues = keyvalue.getOrElse(key, "").split(",")
          if (newValue) {
            selectedCheckboxes += checkbox
            selectedMapping.update(
              key,
              selectedMapping.getOrElse(key, "") + (if (
                                                      selectedMapping
                                                        .get(key)
                                                        .exists(_.nonEmpty)
                                                    ) ","
                                                    else "") + checkbox.text()
            )
            unselectedMapping.get(key).foreach { values =>
              unselectedMapping.update(key, values.replace(checkbox.text(), ""))
            }

            // selectedCheckboxes += checkbox
            // selectedMapping += (key -> (selectedMapping.getOrElse(key, "") + checkbox.text()))
            // unselectedMapping.get(key).foreach { values =>
            //   unselectedMapping += (key -> values.replace(checkbox.text(), ""))
            // }
          } else {
            selectedCheckboxes -= checkbox
            selectedMapping.get(key).foreach { values =>
              selectedMapping += (key -> values.replace(checkbox.text(), ""))
            }

            unselectedMapping.update(
              key,
              unselectedMapping.getOrElse(key, "") + (if (
                                                        unselectedMapping
                                                          .get(key)
                                                          .exists(_.nonEmpty)
                                                      ) ","
                                                      else "") + checkbox.text()
            )

          }
        }
      }
      var checkboxesHBox = new VBox {
        spacing = 10
        children = checkboxes
      }
      var keyVBox = new VBox {
        spacing = 5
        children = Seq(keyLabel, checkboxesHBox)
      }
      tab2vbox.children.append(keyVBox)
    }

    ////////////// Updating Schema //////////////

    def createupdatingTask(): Task[Unit] = Task {
      // Simulating a time-consuming process
      for ((key, values) <- selectedMapping) {
        val valueList = values.split(",")
        for (value <- valueList) {
          st.executeUpdate(
            "UPDATE schema SET varfree = TRUE WHERE tablename='" + key + "'and fieldname='" + value + "';"
          )
          // print("keylist" + keylist +"\n")
          // print("valuelist" + valuelist+"\n")
        }
      }
      // print(deletekeylist.length)
      // print(deletevaluelist.length)

      for ((key, values) <- unselectedMapping) {
        val valueList = values.split(",")
        for (value <- valueList) {
          st.executeUpdate(
            "UPDATE schema SET varfree = FALSE WHERE tablename='" + key + "'and fieldname='" + value + "';"
          )
        }
        // print("deletekeylist" + deletekeylist+"\n")
        // print("deletevaluelist" + deletevaluelist+"\n")

      }
      Thread.sleep(10000)
    }

    val progressBar2 = new ProgressBar {
      prefWidth = 200
      progress = 0
    }

    val loadingLabel2 = new Label("Updating...") {
      visible = false
    }
    val completed2 = new Label("Updating Complete!!") {
      visible = false
    }
    val tab2hbox2 = new HBox {
      spacing = 20
      alignment = Center
      children = Seq(loadingLabel2, completed2)
    }

    val up_schema = new Button("Update Schema") {
      onAction = (e: ActionEvent) => {
        // print("SM" + selectedMapping + "\n")
        // print(unselectedMapping)
        
        val loadingTask = createupdatingTask()

        loadingTask.setOnRunning { _ =>
          progressBar2.progress = -1 // Indeterminate progress
          // completed2.visible = false
          loadingLabel3.visible = false
          loadingLabel2.text = "Updating..."
          loadingLabel2.visible = true

        }

        loadingTask.setOnSucceeded { _ =>
          progressBar2.progress = 1 // Completed progress
          // loadingLabel.text = "Completed!!"
          
          loadingLabel2.text = "Updating Complete!!"
          // completed2.visible = true
          
        }

        new Thread(loadingTask).start()
        
      }
    }
    Tooltip.install(up_schema, new Tooltip("First Update schema, then Refresh Loading"))
    var (loadingLabel3, completed3) = new EquationConversion().LabelCompleted()

    val refreshbutton = new Button("Load")
    {
      onAction = (e: ActionEvent) => {
      ctx1 = Database.loadSchema(conn)
       var alert = new GuiAlert()
        (enc, flagV) match {
          case ("", _) => alert.showWarning("Please select encoding")
          case (_, "") => alert.showWarning("Please select variable")
          case (_, _) => {
            val loadingTask = createLoadingTask()
            

            loadingTask.setOnRunning { _ =>
              progressBar2.progress = -1 // Indeterminate progress
              loadingLabel2.visible = false
              loadingLabel3.visible = true
              loadingLabel3.text = "Loading..."
              // completed3.visible = false

            }

            loadingTask.setOnSucceeded { _ =>
              progressBar2.progress = 1 // Completed progress
              // loadingLabel.text = "Completed!!"
              loadingLabel3.text = "Loading Complete!!"
              // completed3.visible = true
              tab3.disable = false
              tabpane.selectionModel().select(tab3)
            }

            new Thread(loadingTask).start()
          }
        }
      }

    }
    Tooltip.install(refreshbutton, new Tooltip("First Update schema, then Refresh Loading"))

    ////////// Tab 2 /////////////
 
    val hnewbox = new HBox() {
      spacing = 20
      children = Seq(loadingLabel2,loadingLabel3)
    }

       val hnewbox1 = new HBox() {
      spacing = 20
      children = Seq(up_schema, refreshbutton)
    }

    // hnewbox.children.add(up_schema)
    // hnewbox.children.add(reset_schema)
    hnewbox.alignment = Center
    hnewbox1.alignment = Center
    tab2vbox.children.add(progressBar2)
    tab2vbox.children.add(hnewbox)
    tab2vbox.children.add(hnewbox1)

    //////// Tab 3/////////////
    var filelabel = new Label("File Name") {
      layoutX = 150
      layoutY = 510
    }

    // to add filechooser
    val file = new Button("Choose File") {
      layoutX = 150
      layoutY = 490
      onAction = (e: ActionEvent) => {
        val filechooser = new FileChooser
        val selectedfile = filechooser.showOpenDialog(stage)
        if (selectedfile != null) {
          val source = scala.io.Source.fromFile(selectedfile)
          val fileContent = try source.getLines.mkString("\n") finally source.close()
          InputArea.text = fileContent
        }
        input_text = selectedfile.toString()
        val filename = input_text.split('\\')
        filelabel.text = filename(filename.length - 1).capitalize
        // print(input_text)
      }
    }
    file.alignment = Center

    val rbn5 = new Button("RUN") {
      layoutX = 210
      layoutY = 490
      
      onAction = (e: ActionEvent) => {

        try{
        ctx1 = Database.loadSchema(conn) // keeps all the key values
        var alert = new GuiAlert()
        (enc, flagV, flag_null, flag_error, input_text) match {
          case ("", _, _, _, _) => alert.showWarning("Please select encoding")
          case (_, "", _, _, _) => alert.showWarning("Please select variable")
          case (_, _, "", _, _) =>
            alert.showWarning("Please select NULL options")
          case (_, _, _, "", _) =>
            alert.showWarning("Please select Cost Function")
          case (_, _, _, _, "") => alert.showWarning("Please insert input")
          case (_, _, _, _, _) => {
            val result = alert.showalert()
            if (result == ButtonType.Yes) {
              stage.scene = GuiScene2.createScene(
                connector,
                input_text,
                ctx1,
                enc,
                encoding,
                flag_error,
                flag_null.toBoolean,
                () => stage.setScene(scene1)
              )
              stage.show()
              // stage.close()
              // stage.scene = scene1
            }
          }
        }
      }
      catch{
        case Absyn.TypeError(msg) => 
          val errorAlert = new GuiAlert()
          errorAlert.showWarning("\nType error: " + msg)
          println("\nType error: " + msg)
        case VirtualSolver.DataProcessingException(msg) =>
          val errorAlert = new GuiAlert()
          errorAlert.showWarning(msg)
      }
        
      }
    }
    val rbn6 = new Button("EXIT") {
      layoutX = 260
      layoutY = 490
      onAction = (e: ActionEvent) => sys.exit(0)

    }

    //////   Tab1  ///////////
    var tab1vbox = new VBox() {
      padding = geometry.Insets(0, 0, 0, 80)
      // spacing = 10
    }

    // tab1vbox.alignment = Center
    tab1vbox.children = List(
      new Pane { prefHeight = 30 },
      label,
      rbn1,
      rbn2,
      new Pane { prefHeight = 10 },
      label1,
      rbn3,
      rbn4,
      new Pane { prefHeight = 10 },
      label2,
      rbn_null,
      rbn_not_null,
      new Pane { prefHeight = 10 },
      label3,
      rbn_s_error,
      rbn_a_error,
      rbn_value_interval,
      new Pane { prefHeight = 10 },
      tab1hbox,
      next1
    )

    ////////// Tab 2 /////////////
    val tab2scroll = new ScrollPane()
    tab2scroll.content = tab2vbox
    ////////// Tab 3 /////////////
    val tab3hbox = new HBox() {
      spacing = 10
      alignment = Center
    }
    tab3hbox.children = List(InputArea)
    val tab3hbox1 = new HBox() {
      spacing = 10
      alignment = Center
    }
    tab3hbox1.children = List(file, filelabel)

    val tab3hbox2 = new HBox() {
      spacing = 10
      alignment = Center
    }
    tab3hbox2.children = List(rbn5, rbn6)

    var tab3vbox = new VBox() {
      spacing = 10
      // padding = Insets(10)
    }
    tab3vbox.children =
      List(new Pane { prefHeight = 10 }, tab3hbox, tab3hbox1, tab3hbox2)

    //// Tab 4 /////
    val tab4scroll = GuiTabFour.tabfour()


    ///////// TABPANE/////////
    tab1.content = tab1vbox
    tab2.content = tab2scroll
    tab3.content = tab3vbox
    tab4.content = tab4scroll
    tabpane.tabs = List(tab1, tab2, tab3, tab4)
    root = tabpane
  }

  stage.scene = scene1
}
