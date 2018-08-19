package com.abhioncbr.etlFramework.commons.transform

case class DummyRule(rule: TransformationRule, subRules: List[DummyRule])
