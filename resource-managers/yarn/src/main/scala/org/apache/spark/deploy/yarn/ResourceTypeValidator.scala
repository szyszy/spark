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

import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkException}

object ResourceTypeValidator {
  private val ERROR_PREFIX: String = "Error: "

  def validateResourceTypes(sparkConf: SparkConf): Unit = {
    val requestedResources: RequestedResources = new RequestedResources(sparkConf)

    validateDuplicateResourceConfig(requestedResources,
      Seq[ResourceTypeConfigProperties](
        new ResourceTypeConfigProperties("driver", "client", "memory"),
        new ResourceTypeConfigProperties("driver", "client", "cores"),
        new ResourceTypeConfigProperties("driver", "cluster", "memory"),
        new ResourceTypeConfigProperties("driver", "cluster", "cores"),
        new ResourceTypeConfigProperties(role = "executor", resourceType = "memory"),
        new ResourceTypeConfigProperties(role = "executor", resourceType = "cores")))
  }

  private def validateDuplicateResourceConfig(requestedResources: RequestedResources,
                                              resourceTypeConfigProperties:
                                              Seq[ResourceTypeConfigProperties]): Unit = {
    val sb = new mutable.StringBuilder()
    resourceTypeConfigProperties
      .foreach(rtc => {
        val errorMessage = validateDuplicateResourceConfigInternal(requestedResources, rtc)
        if (errorMessage.nonEmpty) {
          printErrorMessageToBuffer(sb, errorMessage)
        }
      })

    if (sb.nonEmpty) {
      throw new SparkException(sb.toString())
    }
  }

  private[spark] def printErrorMessageToBuffer(sb: StringBuilder, str: String) = {
    sb.append(s"$ERROR_PREFIX$str\n")
  }

  private def validateDuplicateResourceConfigInternal(requestedResources: RequestedResources,
                                                      rtc: ResourceTypeConfigProperties): String = {
    val role = rtc.role
    val mode = rtc.mode
    val resourceType = rtc.resourceType

    if (role != "driver" && role != "executor") {
      throw new IllegalArgumentException("Role must be either 'driver' or 'executor'!")
    }
    if (mode != "" && mode != "client" && mode != "cluster") {
      throw new IllegalArgumentException("Mode must be either 'client' or 'cluster'!")
    }
    if (resourceType != "cores" && resourceType != "memory") {
      throw new IllegalArgumentException("Resource type must be either 'cores' or 'memory'!")
    }

    var customResourceTypes: Map[String, String] = null
    (role, mode, resourceType) match {
      case ("executor", _, _) => customResourceTypes = requestedResources
        .customResourceTypesForExecutor
      case ("driver", "client", _) => customResourceTypes = requestedResources
        .customResourceTypesForDriverClientMode
      case ("driver", "cluster", _) => customResourceTypes = requestedResources
        .customResourceTypesForDriverClusterMode
    }

    var resourceTypeObj: String = null
    (role, resourceType) match {
      case ("driver", "cores") => resourceTypeObj = requestedResources.driverCores
      case ("driver", "memory") => resourceTypeObj = requestedResources.driverMemory
      case ("executor", "cores") => resourceTypeObj = requestedResources.executorCores
      case ("executor", "memory") => resourceTypeObj = requestedResources.executorMemory
    }

    val (standardResourceTypeId: String, customResourceTypeId: String) =
      getResourceTypeIdsByRole(role, mode, resourceType)

    if (resourceTypeObj != null && customResourceTypes.contains(customResourceTypeId)) {
      return formatDuplicateResourceTypeErrorMessage(standardResourceTypeId, customResourceTypeId)
    }
    ""
  }

  private def formatDuplicateResourceTypeErrorMessage(standardResourceTypeId: String,
                                                      customResourceTypeId: String): String = {
    s"$standardResourceTypeId and $customResourceTypeId" +
      " configs are both present, only one of them is allowed at the same time!"
  }

  private def getResourceTypeIdsByRole(role: String, mode: String, resourceType: String) = {
    val standardResourceTypeId: String = s"spark.$role.$resourceType"

    var customResourceTypeId: String = ""
    (role, mode) match {
      case ("driver", "client") => customResourceTypeId += "spark.yarn.am.resource."
      case ("driver", "cluster") => customResourceTypeId += "spark.yarn.driver.resource."
      case ("executor", _) => customResourceTypeId += "spark.yarn.executor.resource."
    }

    customResourceTypeId += resourceType

    (standardResourceTypeId, customResourceTypeId)
  }

  private class ResourceTypeConfigProperties(val role: String,
                                             val mode: String = "",
                                             val resourceType: String)


  private class RequestedResources(val sparkConf: SparkConf) {
    val driverMemory: String = safelyGetFromSparkConf(sparkConf, "spark.driver.memory")
    val driverCores: String = safelyGetFromSparkConf(sparkConf, "spark.driver.cores")
    val executorMemory: String = safelyGetFromSparkConf(sparkConf, "spark.executor.memory")
    val executorCores: String = safelyGetFromSparkConf(sparkConf, "spark.executor.cores")
    val customResourceTypesForDriverClientMode: Map[String, String] =
      extractCustomResourceTypes(sparkConf, "spark.yarn.am.resource.")
    val customResourceTypesForDriverClusterMode: Map[String, String] =
      extractCustomResourceTypes(sparkConf, "spark.yarn.driver.resource.")
    val customResourceTypesForExecutor: Map[String, String] =
      extractCustomResourceTypes(sparkConf, "spark.yarn.executor.resource.")

    private def extractCustomResourceTypes(sparkConf: SparkConf,
                                           propertyPrefix: String): Map[String, String] = {
      val result: collection.mutable.HashMap[String, String] =
        new collection.mutable.HashMap[String, String]()

      val propertiesWithPrefix: Array[(String, String)] = sparkConf.getAllWithPrefix(propertyPrefix)
      propertiesWithPrefix.foreach(e => result.put(propertyPrefix + e._1, e._2))

      result.toMap
    }

    private def safelyGetFromSparkConf(sparkConf: SparkConf, key: String): String = {
      try {
        sparkConf.get(key)
      } catch {
        case _: Exception => null
      }
    }
  }

}
