package com.abhioncbr.etlFramework.commons.job

import com.abhioncbr.etlFramework.commons.ProcessFrequencyEnum
import com.abhioncbr.etlFramework.commons.common.GeneralParamConf

case class JobStaticParamConf(processFrequency: ProcessFrequencyEnum.frequencyType,
                              jobName: String, publishStats: Boolean,
                              otherParams: Option[Array[GeneralParamConf]] = None)

