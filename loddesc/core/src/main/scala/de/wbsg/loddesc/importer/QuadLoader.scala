package de.wbsg.loddesc.importer

import ldif.local.datasources.dump.QuadFileLoader
import org.apache.hadoop.mapreduce.{Job, RecordReader}
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit
import org.apache.pig.LoadFunc
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import ldif.runtime.Quad
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.pig.data.{TupleFactory, Tuple}
import java.util.logging.Logger
import ldif.entity.NodeTrait

/**
 * N-Triples, N-Quads loader for Pig
 */
class QuadLoader(val subjTuple: String) extends LoadFunc {
  private val loader = new QuadFileLoader("http://wbsg.de/default", discardFaultyQuads = true)
  private var in: RecordReader[LongWritable, Text] = null
  private val tupleFactory = TupleFactory.getInstance()
  private val logger = Logger.getLogger(getClass.getName)
  private var errorCounter = 0
  private val tupleSubject = if(subjTuple.toLowerCase=="true")
    true
  else
    false

  def this() {
    this("false")
  }

  override def setLocation(location: String, job: Job) {
    FileInputFormat.setInputPaths(job, location)
  }

  override def getInputFormat = new TextInputFormat

  override def prepareToRead(reader: RecordReader[_, _], pigSplit: PigSplit) {
    errorCounter = 0
    in = reader.asInstanceOf[RecordReader[LongWritable, Text]]
  }

  override def getNext: Tuple = {
    var quad: Quad = null
    while (quad == null && in.nextKeyValue())
      try {
        quad = loader.parseQuadLine(in.getCurrentValue.toString)
      } catch {
        case _ => errorCounter += 1
      }
    if (quad == null) {
      if(errorCounter>0)
        logger.warning("Encountered " + errorCounter + " parse errors!")
      return null
    }
    else {
      val subject = createSubjectValue(quad)
      val objTuple = createNodeTuple(quad.value)
      val quadTuple = createTuple(subject, quad.predicate, objTuple, quad.graph)
      return quadTuple
    }
  }


  private def createSubjectValue(quad: Quad): Object = {
    if (tupleSubject) {
      createNodeTuple(quad.subject)
    } else {
      if (quad.subject.isBlankNode)
        quad.subject.toNTriplesFormat
      else
        quad.subject.value
    }
  }

  private def createNodeTuple(node: NodeTrait): Tuple = {
    val objTuple = tupleFactory.newTuple(3)
    objTuple.set(0, node.nodeType.id)
    if(node.isBlankNode)
      objTuple.set(1, node.toNTriplesFormat)
    else
      objTuple.set(1, node.value)
    objTuple.set(2, node.datatypeOrLanguage)
    return objTuple
  }

  private def createTuple(args: Object*): Tuple = {
    val tuple = tupleFactory.newTuple(args.length)
    for(i <- 0 until args.length)
      tuple.set(i, args(i))
    return tuple
  }
}

