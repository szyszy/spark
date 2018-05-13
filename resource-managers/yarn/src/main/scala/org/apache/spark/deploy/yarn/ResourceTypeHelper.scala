/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn

import java.lang.reflect.InvocationTargetException

import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.records.Resource

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * This helper class uses some of Hadoop 3 methods from the yarn API,
 * so we need to use reflection to avoid compile error when building against Hadoop 2.x
 */
object ResourceTypeHelper extends Logging {
  private val AMOUNT_AND_UNIT_REGEX = "([0-9]+)([A-Za-z]*)".r
  private val resourceTypesNotAvailableErrorMessage =
    "Ignoring updating resource with resource types because " +
    "the version of YARN does not support it!"

  def setResourceInfoFromResourceTypes(
      resourceTypesParam: Map[String, String],
      resource: Resource): Resource = {
    if (resource == null) {
      throw new IllegalArgumentException("Resource parameter should not be null!")
    }

    if (!ResourceTypeHelper.isYarnResourceTypesAvailable()) {
      logWarning(resourceTypesNotAvailableErrorMessage)
      return resource
    }

    val resourceTypes = resourceTypesParam.map { case (k, v) => (
      if (k.equals("memory")) {
        logWarning("Trying to use 'memory' as a custom resource, converted it to 'memory-mb'")
        "memory-mb"
      } else k, v)
    }

    logDebug(s"Custom resource types: $resourceTypes")
    resourceTypes.foreach { rt =>
      val resourceName: String = rt._1
      val (amount, unit) = getAmountAndUnit(rt._2)
      logDebug(s"Registering resource with name: $resourceName, amount: $amount, unit: $unit")

      try {
        val resInfoClass = Utils.classForName(
          "org.apache.hadoop.yarn.api.records.ResourceInformation")
        val setResourceInformationMethod =
          resource.getClass.getMethod("setResourceInformation", classOf[String],
            resInfoClass)

        val resourceInformation =
          createResourceInformation(resourceName, amount, unit, resInfoClass)
        setResourceInformationMethod.invoke(resource, resourceName, resourceInformation)
      } catch {
        case e: InvocationTargetException =>
          if (e.getCause != null) {
            throw e.getCause
          } else {
            throw e
          }
        case NonFatal(e) =>
          logWarning(resourceTypesNotAvailableErrorMessage, e)
      }
    }
    resource
  }

  def getCustomResourcesAsStrings(resource: Resource): String = {
    if (resource == null) {
      throw new IllegalArgumentException("Resource parameter should not be null!")
    }

    if (!ResourceTypeHelper.isYarnResourceTypesAvailable()) {
      logWarning(resourceTypesNotAvailableErrorMessage)
      return ""
    }

    var res: String = ""
    try {
      val resUtilsClass = Utils.classForName(
        "org.apache.hadoop.yarn.util.resource.ResourceUtils")
      val getNumberOfResourceTypesMethod = resUtilsClass.getMethod("getNumberOfKnownResourceTypes")
      val numberOfResourceTypes: Int = getNumberOfResourceTypesMethod.invoke(null).asInstanceOf[Int]
      val resourceClass = Utils.classForName(
        "org.apache.hadoop.yarn.api.records.Resource")

      // skip memory and vcores (index 0 and 1)
      for (i <- 2 until numberOfResourceTypes) {
        val getResourceInfoMethod = resourceClass.getMethod("getResourceInformation",
          classOf[Int])
        res ++= getResourceInfoMethod.invoke(resource, i.asInstanceOf[AnyRef]).toString()
      }
    } catch {
      case e: InvocationTargetException =>
        if (e.getCause != null) {
          throw e.getCause
        } else {
          throw e
        }
      case NonFatal(e) =>
        logWarning(resourceTypesNotAvailableErrorMessage, e)

    }
    res
  }

  def getAmountAndUnit(s: String): (Long, String) = {
    try {
      val AMOUNT_AND_UNIT_REGEX(amount, unit) = s
      (amount.toLong, convertToYarnResourceFormat(unit))
    } catch {
      case _: MatchError => throw new IllegalArgumentException(
        s"Value of resource type should match pattern $AMOUNT_AND_UNIT_REGEX, unmatched value: $s")
    }
  }

  def convertToYarnResourceFormat(unit: String): String = {
    unit match {
      case "m" => "M"
      case "g" => "G"
      case "t" => "T"
      case "p" => "P"
      case _ => unit
    }
  }

  private def createResourceInformation(
      resourceName: String,
      amount: Long,
      unit: String,
      resInfoClass: Class[_]) = {
    val resourceInformation =
      if (unit.nonEmpty) {
        val resInfoNewInstanceMethod = resInfoClass.getMethod("newInstance",
          classOf[String], classOf[String], classOf[Long])
        resInfoNewInstanceMethod.invoke(null, resourceName, unit, amount.asInstanceOf[AnyRef])
      } else {
        val resInfoNewInstanceMethod = resInfoClass.getMethod("newInstance",
          classOf[String], classOf[Long])
        resInfoNewInstanceMethod.invoke(null, resourceName, amount.asInstanceOf[AnyRef])
      }
    resourceInformation
  }

  /**
   * Checks whether Hadoop 2.x or 3 is used as a dependency.
   * In case of Hadoop 3 and later,
   * the ResourceInformation class should be available on the classpath.
   */
  def isYarnResourceTypesAvailable(): Boolean = {
    try {
      Utils.classForName("org.apache.hadoop.yarn.api.records.ResourceInformation")
      true
    } catch {
      case NonFatal(e) =>
        logWarning(resourceTypesNotAvailableErrorMessage, e)
        false
    }
  }
}
