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

import java.lang.{Integer => JInteger, Long => JLong}
import java.lang.reflect.InvocationTargetException

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.records.Resource

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * This helper class uses some of Hadoop 3 methods from the YARN API,
 * so we need to use reflection to avoid compile error when building against Hadoop 2.x
 */
private object ResourceTypeHelper extends Logging {
  private val AMOUNT_AND_UNIT_REGEX = "([0-9]+)([A-Za-z]*)".r
  private val RESOURCE_TYPES_NOT_AVAILABLE_ERROR_MESSAGE =
    "Ignoring updating resource with resource types because " +
        "the version of YARN does not support it!"

  def setResourceInfoFromResourceTypes(
      resourceTypes: Map[String, String],
      resource: Resource): Resource = {
    require(resource != null, "Resource parameter should not be null!")

    if (!ResourceTypeHelper.isYarnResourceTypesAvailable() && !resourceTypes.isEmpty()) {
      logWarning(RESOURCE_TYPES_NOT_AVAILABLE_ERROR_MESSAGE)
      return resource
    }

    logDebug(s"Custom resource types: $resourceTypes")
    val resInfoClass = Utils.classForName(
      "org.apache.hadoop.yarn.api.records.ResourceInformation")
    val setResourceInformationMethod =
      resource.getClass.getMethod("setResourceInformation", classOf[String],
        resInfoClass)
    resourceTypes.foreach { case (name, rawAmount) =>
      try {
        val AMOUNT_AND_UNIT_REGEX(amountPart, unitPart) = rawAmount
        val amount = amountPart.toLong
        val unit = unitPart match {
          case "g" => "G"
          case "t" => "T"
          case "p" => "P"
          case _ => unitPart
        }
        logDebug(s"Registering resource with name: $name, amount: $amount, unit: $unit")
        val resourceInformation =
          createResourceInformation(name, amount, unit, resInfoClass)
        setResourceInformationMethod.invoke(resource, name, resourceInformation)
      } catch {
        case _: MatchError => throw new IllegalArgumentException(
          s"Resource request for '$name' ('$rawAmount') does not match pattern $AMOUNT_AND_UNIT_REGEX.")
        case e: InvocationTargetException =>
          if (e.getCause != null) {
            throw e.getCause
          } else {
            throw e
          }
      }
    }
    resource
  }

  private def createResourceInformation(
      resourceName: String,
      amount: Long,
      unit: String,
      resInfoClass: Class[_]): Any = {
    val resourceInformation =
      if (unit.nonEmpty) {
        val resInfoNewInstanceMethod = resInfoClass.getMethod("newInstance",
          classOf[String], classOf[String], JLong.TYPE)
        resInfoNewInstanceMethod.invoke(null, resourceName, unit, amount.asInstanceOf[JLong])
      } else {
        val resInfoNewInstanceMethod = resInfoClass.getMethod("newInstance",
          classOf[String], JLong.TYPE)
        resInfoNewInstanceMethod.invoke(null, resourceName, amount.asInstanceOf[JLong])
      }
    resourceInformation
  }

  /**
   * Checks whether Hadoop 2.x or 3 is used as a dependency.
   * In case of Hadoop 3 and later, the ResourceInformation class
   * should be available on the classpath.
   */
  def isYarnResourceTypesAvailable(): Boolean = {
    try {
      Utils.classForName("org.apache.hadoop.yarn.api.records.ResourceInformation")
      true
    } catch {
      case NonFatal(e) =>
        false
    }
  }
}
