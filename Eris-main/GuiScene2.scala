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
import scala.io.Source
import scalafx.scene.paint.Color
import scalafx.geometry.Insets
import javafx.geometry
import scalafx.scene.control.TableView
import scalafx.collections.ObservableBuffer
import scalafx.scene.layout.Region
import scalafx.scene.text.Text

object GuiScene2 extends JFXApp {
  def createScene(
      connector: Connector,
      input_text: String,
      boundlist: ListBuffer[Double],
      ctx1: Database.InstanceSchema,
      enc: String,
      encoding: Encoding,
      flag_error: String,
      flag_null: Boolean,
      goToScene1: () => Unit
  ): Scene = {
    val tabpane = new TabPane()
    val tab1 = new Tab()
    tab1.text = "Encoded SQL Query" // Result Schema"
    val tab2 = new Tab {
      text = "Query Results"
      // content = tableView
    }
    // tab2.text = "Query Results" //Query Encoding"
    val tab3 = new Tab()
    tab3.text = "Constraints" // Query Encoding Schema"
    val tab4 = new Tab()
    tab4.text = "Valuations of the variables" // Query Encoding SQL"
    val tab5 = new Tab()
    tab5.text = "Discords"
    // val tab6 = new Tab()
    // tab6.text = "Base Results VC"

    val test = GuiMain
    var (schema,q0,q0vc,schema0,schema0vc,sql0,sql0vc,result0,result0vc,eq) 
    = test.queryprint(connector, input_text, boundlist, ctx1, enc, encoding)

    var (schema1,q01,q0vc1,schema01,schema0vc1,sql01,sql0vc1,result01,result0vc1,eq1) 
    = test.queryprint(connector, input_text, boundlist, ctx1, "nf2_sparsev", Encoding.encoder_to_use("nf2_sparsev"))


    var (valuation, objective, eqs, vars, eqCreationTime, solveTime) =
      test.costprint(
        connector,
        input_text,
        boundlist,
        ctx1,
        encoding,
        flag_error,
        flag_null
      )
    var ueq = new EquationConversion()

    // tasks for tab1

    val tab1lbl1 = new Label(ueq.queryconvert(sql0))
    val tab1lbl2 = new Label(ueq.queryconvert(sql0))
    val tab1vbox = new VBox() {
      spacing = 30
      padding = Insets(0, 0, 0, 30)
      prefHeight = 300
      prefWidth = 300
      alignment = Center
    }
    tab1vbox.alignment = Center
    tab1vbox.children.append(new Pane { prefHeight = 20 })
    // tab4vbox.children = List(tab4lbl1, tab4lbl2)
    val tab1border = new BorderPane {
      top = tab1lbl1
      center = tab1lbl2
      prefHeight = 290
    }
    val tab1scroll = new ScrollPane
    tab1scroll.content = tab1border
    tab1vbox.children.append(tab1scroll)
    tab1vbox.children.append(ueq.button(goToScene1))

    tab1.content = tab1vbox

    // tasks for tab2
    val tab2lbl1 = new Label(result0.toString())
    // print(tab2lbl1)
    val tab2lbl2 = new Label(result0vc.toString())
    val tab2vbox = new VBox() {
      spacing = 10
      padding = Insets(0, 0, 0, 30)
      prefHeight = 450
      prefWidth = 350
      alignment = Center
    }
    tab2vbox.children.append(new Pane { prefHeight = 20 })

    // parse the schemao
    val tab2Column =
      schema0.toString.split(";").flatMap(_.split(",")).filter(_.nonEmpty)
    // print("tab2olumn "  + tab2Column.to\l)
    // Case class for data rows
    case class DataRow(values: Map[String, String])

    val rows: List[String] = result01.toString().split('\n').toList
    print(result0)
    print("\n")
    

    // Parse data into DataRow format
    val parsedData = rows.map { row =>
      val rowValues = row.split(";")
      // print(rowValues.toList)
      val firstPartList = rowValues(0).split(",").toList
      val last = new EquationConversion().transformTableLast(rowValues.last)
      val combinedList =
        firstPartList ++ List(last) // Combine into a single list
      val rowMap = tab2Column.zip(combinedList).toMap
      DataRow(rowMap)
    }
    // Observable buffer of parsed data
    val observableData = ObservableBuffer[DataRow]() ++= parsedData

    // Create TableView with dynamic columns
    // Assuming columns is the mutable collection from the TableView
    val tableView = new TableView[DataRow](observableData)
    tab2Column.foreach { columnName =>
      val column = new TableColumn[DataRow, String] {
        text = columnName
        cellValueFactory = { dataRow =>
          val cellValue = dataRow.value.values.getOrElse(columnName, "")
          // println(s"Extracting: $columnName -> $cellValue")  // Debug output
          scalafx.beans.property.ReadOnlyObjectWrapper(cellValue)
        }
      }
      tableView.columns += column
    }

    val tab2border = new BorderPane {
      top = tableView
      prefHeight = 290
    }
    val tab2scroll = new ScrollPane
    tab2scroll.content = tab2border
    tab2scroll.fitToHeightProperty() = true
    tab2scroll.fitToWidthProperty() = true
    tab2vbox.children.append(tab2scroll)
    tab2vbox.children.append(ueq.button(goToScene1))
    tab2.content = tab2scroll

    // tasks for tab3

    var eqlength = eq.length
    var tab3vbox = new VBox() {
      spacing = 10
      padding = Insets(0, 0, 0, 20)
      alignment = Center
    }
    var expressionsPerPage: Int = 15
    var pageIndex: Int = 0
    var c = eq
    var expressionLabel: Label = new Label {
      text = ueq.convertExpression(c.take(expressionsPerPage).mkString("\n"))

    }

    val tab3border = new BorderPane {
      top = expressionLabel
      prefHeight = 290
      // prefWidth = 350
    }
    tab3vbox.children.append(new Pane { prefHeight = 20 })
    // tab6vbox.children.append(expressionLabel)

    var tab3scroll = new ScrollPane
    tab3scroll.content = tab3border
    tab3vbox.children.append(tab3scroll)

    lazy val backButton: Button = new Button {
      text = "back"
      disable = true
      onAction = (_: ActionEvent) => {
        pageIndex -= 1
        updateEquationLabel()
      }
    }

    lazy val nextButton: Button = new Button {
      text = "next"
      disable = eqlength <= expressionsPerPage
      onAction = (_: ActionEvent) => {
        pageIndex += 1
        updateEquationLabel()
      }
    }

    def updateEquationLabel(): Unit = {
      val start = pageIndex * expressionsPerPage
      val end = start + expressionsPerPage
      expressionLabel.text =
        ueq.convertExpression(eq.slice(start, end).mkString("\n"))
      backButton.disable = pageIndex == 0
      nextButton.disable = end >= eqlength
    }

    var flowpane: FlowPane = new FlowPane {
      hgap = 10
      // padding = Insets(0,0,0,50)
      alignment = Center
    }
    flowpane.children.append(backButton)
    flowpane.children.append(nextButton)
    tab3vbox.children.append(flowpane)

    // tab6vbox.children.append(new Pane{prefHeight=60})
    tab3vbox.children.append(ueq.button(goToScene1))
    tab3.content = tab3vbox

    // tasks for tab4
    val tab4vbox = new VBox {
      spacing = 10
      padding = Insets(0, 0, 0, 30)
      alignment = Center
    }
    tab4vbox.children.append(new Pane { prefHeight = 20 })
    val tab4border = new BorderPane {

      top = new Label(
        valuation.toString
          .replaceAll("\\),", "\\)\n")
          .replace("List(", "")
          .replace("))", ")")
      )
      // bottom = new Label("Cost Result  " + objective.toString())
      prefHeight = 290
    }
    val tab4scroll = new ScrollPane
    tab4scroll.content = tab4border
    tab4vbox.children.append(tab4scroll)
    tab4vbox.children.append(ueq.button(goToScene1))
    // tab5vbox.children = List (new Label(valuation.toString()), new Label(objective.toString()))
    tab4.content = tab4vbox

    // tasks for tab5

    val tab5hbox = new HBox {
      spacing = 10
      padding = Insets(0, 0, 0, 30)
      alignment = Center
    }
    tab5hbox.children.append(new Pane { prefHeight = 20 })
    tab5hbox.children.append(
      new Label("Discord among the sources  " + objective.toString())
    )
    tab5.content = tab5hbox

    //////// TABPANE /////////
    tabpane.tabs = List(tab1, tab2, tab3, tab4, tab5)

    new Scene(600, 450) {
      fill = Color.LightGray
      root = tabpane

    }
  }
}
