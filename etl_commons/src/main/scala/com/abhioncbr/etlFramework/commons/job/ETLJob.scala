package com.abhioncbr.etlFramework.commons.job

import com.abhioncbr.etlFramework.commons.extract.Extract
import com.abhioncbr.etlFramework.commons.load.Load
import com.abhioncbr.etlFramework.commons.transform.Transform

case class ETLJob(jobStaticParam: JobStaticParam, extract: Extract, transform: Transform, load: Load)










