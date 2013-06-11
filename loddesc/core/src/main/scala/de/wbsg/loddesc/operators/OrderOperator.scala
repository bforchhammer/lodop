package de.wbsg.loddesc.operators

import de.wbsg.loddesc.model.{Operator, Group}


class OrderOperator extends Operator{
  private var orderBy : String = ""
  private var group : Group = null

  override def toPig() : String = {
    return "ORDER "+ group.toString +" by "+ orderBy +";"
  }

}