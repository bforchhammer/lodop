package de.wbsg.loddesc.operators

import de.wbsg.loddesc.model.{Group, Operator}

class FilterOperator extends Operator {
  private var group : Group = null
  private var key : String = ""
  private var value : String = ""

  override def toPig() : String = {
    return "FILTER " + group.toString + " BY " + key + " == '" + value + "';"
  }


}