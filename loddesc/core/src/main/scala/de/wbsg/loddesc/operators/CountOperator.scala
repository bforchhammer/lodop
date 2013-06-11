package de.wbsg.loddesc.operators

import de.wbsg.loddesc.model.{Group, Operator}

class CountOperator extends Operator {
  private var group : Group = null

  override def toPig() : String = {
    return "COUNT ("+ group.toString +");"
  }

}