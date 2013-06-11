package flow

import org.apache.pig.{ExecType, PigServer}
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: andreas
 * Date: 6/13/12
 * Time: 11:47 AM
 * To change this template use File | Settings | File Templates.
 */

object Main {
  def main(args: Array[String]) {
    if(args.length < 2) {
      println("Arguments: <input path> <output path> [pig script]")
      sys.exit(1)
    }
    val script = if(args.length>2) args(2) else "pigscripts/stats.pig"
    runPigScripts(script, args(0), args(1))
  }

  def runPigScripts(scriptPath: String, input: String, output: String) {
    runPigScript(scriptPath, Map("FILE" -> input, "results" -> output))
  }

  def runPigScript(scriptPath: String, parameters: Map[String, String]) {
      val pigServer = new PigServer("local")
      pigServer.registerScript("pigscripts/stats.pig", parameters)
      pigServer.shutdown()
    }
}
