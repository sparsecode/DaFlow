package com.abhioncbr.etlFramework.metrics.stats

case class JobResult(success: Boolean, subtask: String,
                     transformationPassedCount: Long,
                     transformationFailedCount: Long,
                     validateCount: Long, nonValidatedCount: Long, failureReason: String)

