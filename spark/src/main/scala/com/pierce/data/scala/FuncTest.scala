package com.pierce.data.scala

/**
  *
  */
object FuncTest {

  def main(args: Array[String]): Unit = {
    val arr:Array[Int] = Array(34, 12, 11)
    println("result:" + sumByFor(arr))
    println("result:" + sumByForeach(arr))
  }

  def sumByFor(arr: Array[Int]): Int = {
    var total = 0
    for (i <- arr.indices) {
      total += arr(i)
    }
    total
  }

  def sumByForeach(arr: Array[Int]): Int = {
    var total = 0
    arr.foreach(item => total += item)
    total
  }
}
