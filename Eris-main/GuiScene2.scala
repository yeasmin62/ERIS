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

object GuiScene2 extends JFXApp {
  def createScene(connector: Connector,
      input_text: String,
      ctx1: Database.InstanceSchema,
      enc:String,
      encoding: Encoding,
      flag_error:String,
      flag_null:Boolean,
      goToScene1: () => Unit): Scene = {
    val tabpane = new TabPane()
    val tab1 = new Tab()
    tab1.text = "Encoded Query" //Result Schema"
    val tab2 = new Tab()
    tab2.text = "Encoded SQL Query" //Query Encoding"
    val tab3 = new Tab()
    tab3.text = "Base Result" //Query Encoding Schema"
    val tab4 = new Tab()
    tab4.text = "Base Results VC" //Query Encoding SQL"
    val tab5 = new Tab()
    tab5.text = "Cost Function Results"
    // val tab6 = new Tab()
    // tab6.text = "Base Results VC"
    
 

    val test = GuiMain
    var (schema, q0, q0vc,schema0, schema0vc, sql0, sql0vc, result0, result0vc, eq) =
        test.queryprint(connector, input_text, ctx1, enc,encoding)

    var (valuation, objective, eqs, vars, eqCreationTime, solveTime) = test.costprint(connector,input_text,ctx1,encoding, flag_error, flag_null)
    var ueq = new EquationConversion()
 
    // tasks for tab 1
    // print(q0)
    val tab1lbl1 = new Label(ueq.queryconvert(q0.toString()))
    val tab1lbl2 = new Label(ueq.queryconvert(q0vc.toString()))
    lazy val tab1vbox = new VBox()
    {
      spacing = 30
      padding = Insets(0,0,0,30)
      alignment = Center
      prefHeight = 290
      prefWidth = 290
    }
    val tab1border = new BorderPane{
      top = tab1lbl1
      center = tab1lbl2
      prefHeight = 290
    }
    val tab1scroll = new ScrollPane
    tab1scroll.content = tab1border
    tab1vbox.children.append(tab1scroll)
    tab1vbox.children.append(ueq.button(goToScene1))
    tab1vbox.alignment = Center
    // tab2vbox.children = List(new Pane{prefHeight=10},tab2lbl1, tab2lbl2, ueq.button(goToScene1))
    // val tab2scroll = new ScrollPane
    // tab2scroll.content = tab2vbox
    tab1.content = tab1vbox


    //tasks for tab2

    val tab2lbl1 = new Label(ueq.queryconvert(sql0))
    val tab2lbl2 = new Label(ueq.queryconvert(sql0))
    val tab2vbox = new VBox()
    {
      spacing = 30
      padding = Insets(0,0,0,30)
      prefHeight = 300
      prefWidth = 300
      alignment = Center
    }
    tab2vbox.alignment = Center
    tab2vbox.children.append(new Pane{prefHeight=20})
    // tab4vbox.children = List(tab4lbl1, tab4lbl2)
    val tab2border = new BorderPane{
      top = tab2lbl1
      center = tab2lbl2
      prefHeight = 290
    }
    val tab2scroll = new ScrollPane
    tab2scroll.content = tab2border
    tab2vbox.children.append(tab2scroll)
    tab2vbox.children.append(ueq.button(goToScene1))
  
    tab2.content = tab2vbox

    //tasks for tab3

    val tab3lbl1 = new Label(result0.toString())
    val tab3lbl2 = new Label(result0vc.toString())
    val tab3vbox = new VBox()
    {
      spacing = 10
      padding = Insets(0,0,0,30)
      prefHeight = 300
      prefWidth = 300
      alignment = Center
    }
    tab3vbox.children.append(new Pane{prefHeight=20})
    val tab3border = new BorderPane{
      top = tab3lbl1
      center = tab3lbl2
      prefHeight = 290
    }
    val tab3scroll = new ScrollPane
    tab3scroll.content = tab3border
    tab3vbox.children.append(tab3scroll)
    tab3vbox.children.append(ueq.button(goToScene1))
    // tab5vbox.children = List(new Pane{prefHeight=10}, tab5lbl1, tab5lbl2,ueq.button(goToScene1))
    // val tab5scroll = new ScrollPane
    // tab5scroll.content = tab5vbox
    tab3.content = tab3vbox

    // tasks for tab4
    
    var eqlength = eq.length
    var tab4vbox = new VBox()
    {
      spacing = 10
      padding = Insets(0,0,0,20)
      alignment = Center
    }
    var expressionsPerPage: Int = 15
    var pageIndex: Int = 0
    var c = eq
    var expressionLabel: Label = new Label {
      text = ueq.convertExpression(c.take(expressionsPerPage).mkString("\n"))
 
    }

    val tab4border = new BorderPane{
      top = expressionLabel
      prefHeight = 290
      // prefWidth = 350
    }
    tab4vbox.children.append(new Pane{prefHeight = 20})
    // tab6vbox.children.append(expressionLabel)

    var tab4scroll = new ScrollPane
    tab4scroll.content = tab4border
    tab4vbox.children.append(tab4scroll)

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
      expressionLabel.text = ueq.convertExpression(eq.slice(start, end).mkString("\n"))
      backButton.disable = pageIndex == 0
      nextButton.disable = end >= eqlength
    }

    var flowpane: FlowPane = new FlowPane{
      hgap = 10
      // padding = Insets(0,0,0,50)
      alignment = Center
    }
    flowpane.children.append(backButton)
    flowpane.children.append(nextButton)
    tab4vbox.children.append(flowpane)

   
    // tab6vbox.children.append(new Pane{prefHeight=60})
    tab4vbox.children.append(ueq.button(goToScene1))
    tab4.content = tab4vbox

    // tasks for tab5
    val tab5vbox = new VBox{
      spacing = 10
      padding = Insets(0,0,0,30)
      alignment = Center
    }
    tab5vbox.children.append(new Pane{prefHeight=20})
    val tab5border = new BorderPane{

      top = new Label(valuation.toString.replaceAll("\\),","\\)\n").replace("List(", "").replace("))", ")"))
      bottom = new Label("Cost Result  " + objective.toString())
      prefHeight = 290
    }
    val tab5scroll = new ScrollPane
    tab5scroll.content = tab5border
    tab5vbox.children.append(tab5scroll)
    tab5vbox.children.append(ueq.button(goToScene1))
    // tab5vbox.children = List (new Label(valuation.toString()), new Label(objective.toString()))
    tab5.content = tab5vbox

    //////// TABPANE /////////
    tabpane.tabs = List(tab1, tab2, tab3, tab4,tab5)

    new Scene(400, 400) {
      fill = Color.LightGray
      root = tabpane

    }
  }
}