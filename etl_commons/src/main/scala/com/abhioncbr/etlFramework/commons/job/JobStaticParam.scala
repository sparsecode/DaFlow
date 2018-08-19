package com.abhioncbr.etlFramework.commons.job

import com.abhioncbr.etlFramework.commons.ProcessFrequencyEnum

case class JobStaticParam(processFrequency: ProcessFrequencyEnum.frequencyType, feedName: String)

