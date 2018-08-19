package com.abhioncbr.etlFramework.commons

object Context {
  private var contextualObjects = Map[ContextConstantEnum.constant, Any]()

  def addContextualObject[T](key: ContextConstantEnum.constant, obj: T): Unit = {
    contextualObjects +=(key -> obj)
  }

  def getContextualObject[T](key: ContextConstantEnum.constant): T = {
    val output = contextualObjects.getOrElse(key,None)
    output.asInstanceOf[T]
  }
}
