package com.lzd.etlFramework.etl.feed.common

object ProcessFrequencyEnum extends Enumeration {
  type frequencyType = Value
  val HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY, DATE_RANGE = Value

  def getProcessFrequencyEnum(frequencyString: String): ProcessFrequencyEnum.frequencyType = {
    val processFrequencyEnum = frequencyString match {
      case "HOURLY" => ProcessFrequencyEnum.HOURLY
      case "DAILY" => ProcessFrequencyEnum.DAILY
      case "WEEKLY" => ProcessFrequencyEnum.WEEKLY
      case "MONTHLY" => ProcessFrequencyEnum.MONTHLY
      case "YEARLY" => ProcessFrequencyEnum.YEARLY
      case "DATE_RANGE"  => ProcessFrequencyEnum.DATE_RANGE
    }
    processFrequencyEnum
  }
}