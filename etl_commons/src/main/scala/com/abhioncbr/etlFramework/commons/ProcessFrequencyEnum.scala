package com.abhioncbr.etlFramework.commons

object ProcessFrequencyEnum extends Enumeration {
  type frequencyType = Value
  val ONCE, HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY, DATE_RANGE = Value

  def getProcessFrequencyEnum(frequencyString: String): ProcessFrequencyEnum.frequencyType = {
    val processFrequencyEnum = frequencyString match {
      case "ONCE" => ProcessFrequencyEnum.ONCE
      case "HOURLY" => ProcessFrequencyEnum.HOURLY
      case "DAILY" => ProcessFrequencyEnum.DAILY
      case "WEEKLY" => ProcessFrequencyEnum.WEEKLY
      case "MONTHLY" => ProcessFrequencyEnum.MONTHLY
      case "YEARLY" => ProcessFrequencyEnum.YEARLY
      case "DATE_RANGE"  => ProcessFrequencyEnum.DATE_RANGE
      case _ =>   throw new RuntimeException(s"'$frequencyString', process frequency not supported.")
    }
    processFrequencyEnum
  }
}