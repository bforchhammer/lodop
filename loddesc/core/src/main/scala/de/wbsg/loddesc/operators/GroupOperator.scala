package de.wbsg.loddesc.operators

import de.wbsg.loddesc.model.{Group, Operator}


class GroupOperator extends Operator {

  private var groupBy : String = ""
  private var group : Group = null

  override def toPig() : String = {
    return "GROUP "+ group.toString +" by "+ groupBy +";"
  }

}