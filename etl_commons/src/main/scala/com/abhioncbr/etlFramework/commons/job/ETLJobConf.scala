package com.abhioncbr.etlFramework.commons.job

import com.abhioncbr.etlFramework.commons.extract.ExtractConf
import com.abhioncbr.etlFramework.commons.load.LoadConf
import com.abhioncbr.etlFramework.commons.transform.TransformConf

case class ETLJobConf(jobStaticParam: JobStaticParamConf, extract: ExtractConf, transform: TransformConf, load: LoadConf)










