import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.geometry.Insets
import scalafx.scene.Scene
import scalafx.scene.control.Label
import scala.collection.mutable.ListBuffer
import scalafx.scene.layout.BorderPane
import scala.io.Source
import javafx.scene.control.ScrollPane
import scalafx.scene.layout.{GridPane, VBox}
import scalafx.scene.control.Alert.AlertType
import scalafx.scene.control.{Alert, ButtonType, Button}
import scalafx.event.ActionEvent
import javafx.scene.layout.FlowPane
import scalafx.scene.layout

object GuiScene1 extends JFXApp {
  def printmsg(
      connector: Connector,
      input_text: String,
      boundlist: ListBuffer[Double],
      ctx1: Database.InstanceSchema,
      enc:String,
      encoding: Encoding,
      goToScene1: () => Unit
  ): Scene = {
    var scene_new = new Scene(400, 400) {
      var d = 40
      var lbl = new Label("Queries") {
        layoutX = 40
        layoutY = d
      }
      d = d + 40

      val test = GuiMain
      var (schema, q0, q0vc,schema0, schema0vc, sql0, sql0vc, result0, result0vc, eq) =
        test.queryprint(connector, input_text,boundlist, ctx1,enc, encoding)
      // var (valuation, objective, eqs, vars, eqCreationTime, solveTime) =
      //   test.costprint(connector, input_text, ctx, encoding)
      // print(result0)

      // }
      var vbox = new VBox()
      var eqlength = eq.length
      // print("eqlength" + eqlength)
      // for (i <- 0 to 10) {
      //   val c = new EquationConversion().convertExpression(eq(i).toString())
      //   var lbl2 = new Label(c) {
      //     layoutX = 40
      //     layoutY = d
      //   }
      //   d = d + 40
      //   vbox.children.append(lbl2)

      // }
      var expressionsPerPage: Int = 10
      var pageIndex: Int = 0
      var ueq = new EquationConversion()
      var c = eq
      var expressionLabel: Label = new Label {
        text = ueq.convertExpression(c.take(expressionsPerPage).mkString("\n"))
 
      }
      vbox.children.append(expressionLabel)

      val backButton: Button = new Button {
        text = "Back"
        disable = true
        onAction = (_: ActionEvent) => {
          pageIndex -= 1
          // updateEquationLabel()
        }
      }

      val nextButton: Button = new Button {
        text = "Next"
        disable = eqlength <= expressionsPerPage
        onAction = (_: ActionEvent) => {
          pageIndex += 1
          // updateEquationLabel()
        }
      }
      def updateEquationLabel(pageIndex:Int, expressionsPerPage:Int, expressionLabel:Label, backButton:Button, nextButton:Button): Unit = {
        val start = pageIndex * expressionsPerPage
        val end = start + expressionsPerPage
        expressionLabel.text = ueq.convertExpression(eq.slice(start, end).mkString("\n"))
        backButton.disable = pageIndex == 0
        nextButton.disable = end >= eqlength
      }

      var flowpane: FlowPane = new FlowPane 
      flowpane.children.append(backButton)
      flowpane.children.append(nextButton)
      vbox.children.append(flowpane)

      val back = new Button("HOME") {
        layoutX = 50
        layoutY = 50
        // onAction = (e: ActionEvent) => {
        //   stage.scene = GuiNew.stage/.
        // }
      }
      back.setOnAction(_ => goToScene1())
      vbox.children.append(back)

      // var expressionLabel: Label = new Label {
      //   text = new EquationConversion().convertExpression(
      //     eq.take(expressionsPerPage).toString
      //   )
      // }

      // var lbl2 = new Label(eq.toString) {
      //   layoutX = 40
      //   layoutY = d
      // }
      // d = d + 40
      // var lbl3 = new Label(q0.toString) {
      //   layoutX = 40
      //   layoutY = d
      // }
      // d = d + 40
      // var lbl4 = new Label(q0vc.toString) {
      //   layoutX = 40
      //   layoutY = d
      // }

      // d = d + 40
      // var lbl5 = new Label(sql0.toString) {
      //   layoutX = 40
      //   layoutY = d
      // }

      // d = d + 20
      // var lbl6 = new Label(sql0vc.toString) {
      //   layoutX = 40
      //   layoutY = d
      // }
      // d = d + 40
      // // }
      // var lbl9 = new Label(result0.toString) {
      //   layoutX = 40
      //   layoutY = d
      // }

      // vbox.children = List(lbl, lbl2, lbl3, lbl4, lbl5, lbl6, lbl9)

      var scrollpane = new ScrollPane()
      scrollpane.content = vbox
      // content = List(lbl,lbl2,lbl3,lbl4,lbl5,lbl6)
      // val lbl = new Label(testresult)
      root = scrollpane
    }
    scene_new
  }
  def updateEquationLabel(pageIndex:Int, expressionsPerPage:Int, expressionLabel:Label, backButton:Button, nextButton:Button, eq:List[Database.Equation], eqlength:Int): Unit = {
        val start = pageIndex * expressionsPerPage
        val end = start + expressionsPerPage
        expressionLabel.text = new EquationConversion().convertExpression(eq.slice(start, end).mkString("\n"))
        backButton.disable = pageIndex == 0
        nextButton.disable = end >= eqlength
      }
}
