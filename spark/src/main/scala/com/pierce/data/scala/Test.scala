package com.pierce.data.scala

/**
  */
object Test {

  def main(args: Array[String]): Unit = {
//    val tupleVar = (true, 11)
//    println(tupleVar)

    // 多维数组(3行4列)
    var multiArr = Array.ofDim[Int](3, 4)
    for (i <- 0 to 2) {
      for (j <- 0 to 3) {
        multiArr(i)(j) = j
      }
    }
    println(multiArr.length)
    println(multiArr.indices)
    var range = Range(0, 2)
    println(range)
//    for (i <- multiArr.indices) {}
//    multiArr.foreach(row => row.foreach(col => println(col)))
  }
}
