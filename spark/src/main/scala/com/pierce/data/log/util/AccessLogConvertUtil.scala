package com.pierce.data.log.util

object AccessLogConvertUtil {

  def parseLog(line: String) = {
    val splits = line.split("\t")
    val ip = splits(0)
    val time = splits(3) + " " + splits(4)
  }
}
