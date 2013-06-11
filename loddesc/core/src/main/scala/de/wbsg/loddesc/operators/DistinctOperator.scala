package de.wbsg.loddesc.operators

import de.wbsg.loddesc.model.{Group, Operator}

class DistinctOperator  extends Operator {
  private var group : Group = null
  private var element : String = ""

  override def toPig() : String = {
    return "DISTINCT "+ group.toString +"."+ element.toString +";"
  }

}