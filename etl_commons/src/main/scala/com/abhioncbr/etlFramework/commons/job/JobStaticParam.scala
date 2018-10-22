package com.abhioncbr.etlFramework.commons.job

import com.abhioncbr.etlFramework.commons.ProcessFrequencyEnum
import com.abhioncbr.etlFramework.commons.common.GeneralParam

case class JobStaticParam(processFrequency: ProcessFrequencyEnum.frequencyType,
                          feedName: String, publishStats: Boolean,
                          otherParams: Option[Array[GeneralParam]] = None)

