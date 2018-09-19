package com.pierce.data.scala

/**
  *
  */
object ListTest {

  def main(args: Array[String]): Unit = {
    val list = List("aa", "bb", "cc")
    val dimList = List(
      List(11, 22, 33),
      List(44, 55, 66),
      List(77, 88, 99)
    )
    val list1 = "HaHa" :: ("HeHe" :: Nil)
    println(list)
    println(dimList)
    println(list1)
  }
}
