package de.wbsg.loddesc.functions

import org.apache.pig.EvalFunc
import org.apache.pig.data.Tuple

class LetterRatio extends EvalFunc[java.lang.Double] {
  def exec(input: Tuple): java.lang.Double = {
    if (input == null || input.size() == 0)
      return null;
    val str = input.get(0).toString
    var letterCount = 0
    for (c <- str if c.isLetter)
      letterCount += 1
    if(str.length==0)
      return 0.0
    else
      return letterCount.toDouble / str.length
  }
}
