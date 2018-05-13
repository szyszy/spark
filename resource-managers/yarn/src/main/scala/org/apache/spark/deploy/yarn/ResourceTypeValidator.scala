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
import org.apache.spark.deploy.yarn.ProcessType.{am, driver, executor, ProcessType}
import org.apache.spark.deploy.yarn.ResourceType.{cores, memory, ResourceType}
import org.apache.spark.deploy.yarn.RunMode.{client, cluster, RunMode}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.config._

private[spark] object ProcessType extends Enumeration {
  type ProcessType = Value
  val driver, executor, am = Value
}

private[spark] object RunMode extends Enumeration {
  type RunMode = Value
  val client, cluster = Value
}

private[spark] object ResourceType extends Enumeration {
  type ResourceType = Value
  val cores, memory = Value
}

private[spark] object ResourceTypeValidator {
  private val ERROR_PREFIX: String = "Error: "
  private val POSSIBLE_RESOURCE_DEFINITIONS = Seq[ResourceConfigProperties](
    new ResourceConfigProperties(am, client, memory),
    new ResourceConfigProperties(am, client, cores),
    new ResourceConfigProperties(driver, cluster, memory),
    new ResourceConfigProperties(driver, cluster, cores),
    new ResourceConfigProperties(processType = executor, resourceType = memory),
    new ResourceConfigProperties(processType = executor, resourceType = cores))

  /**
   * Validates sparkConf and throws a SparkException if a standard resource (memory or cores)
   * is defined with the property spark.yarn.x.resource.y<br>
   *
   * Example of an invalid config:<br>
   * - spark.yarn.driver.resource.memory=2g<br>
   *
   * Please note that if multiple resources are defined like described above,
   * the error messages will be concatenated.<br>
   * Example of such a config:<br>
   * - spark.yarn.driver.resource.memory=2g<br>
   * - spark.yarn.executor.resource.cores=2<br>
   * Then the following two error messages will be printed:<br>
   * - "memory cannot be requested with config spark.yarn.driver.resource.memory,
   * please use config spark.driver.memory instead!<br>
   * - "cores cannot be requested with config spark.yarn.executor.resource.cores,
   * please use config spark.executor.cores instead!<br>
   *
   * @param sparkConf
   */
  def validateResources(sparkConf: SparkConf): Unit = {
    val requestedResources = new RequestedResources(sparkConf)
    val sb = new mutable.StringBuilder()
    POSSIBLE_RESOURCE_DEFINITIONS.foreach { rcp =>
      val customResources: Map[String, String] = getCustomResourceValue(requestedResources, rcp)
      val (standardResourceConfigKey: String, customResourceConfigKey: String) =
        getResourceConfigKeys(rcp)

      val errorMessage =
        if (customResources.contains(customResourceConfigKey)) {
          s"${rcp.resourceType} cannot be requested with config $customResourceConfigKey, " +
              s"please use config $standardResourceConfigKey instead!"
        } else {
          ""
        }
      if (errorMessage.nonEmpty) {
        printErrorMessageToBuffer(sb, errorMessage)
      }
    }

    if (sb.nonEmpty) {
      throw new SparkException(sb.toString())
    }
  }

  /**
   * Returns the requested map of custom resources,
   * based on the ResourceConfigProperties argument.
   * @return
   */
  private def getCustomResourceValue(
      requestedResources: RequestedResources,
      rcp: ResourceConfigProperties) = {
    var customResources: Map[String, String] = null
    (rcp.processType, rcp.runMode, rcp.resourceType) match {
      case (ProcessType.executor, _, _) => customResources =
          requestedResources.customExecutorResources
      case (ProcessType.am, RunMode.client, _) => customResources =
          requestedResources.customAMResources
      case (ProcessType.driver, RunMode.cluster, _) => customResources =
          requestedResources.customDriverResources
    }
    customResources
  }

  /**
   * Returns a tuple of standard resource config key and custom resource config key, based on the
   * processType and runMode fields of the ResourceConfigProperties argument.
   * Standard resources are memory and cores.
   *
   * @param rcp
   * @return
   */
  private def getResourceConfigKeys(rcp: ResourceConfigProperties): (String, String) = {
    val standardResourceConfigKey: String = if (rcp.processType == ProcessType.am) {
      s"spark.yarn.${rcp.processType}.${rcp.resourceType}"
    } else {
      s"spark.${rcp.processType}.${rcp.resourceType}"
    }

    var customResourceTypeConfigKey: String = ""
    (rcp.processType, rcp.runMode) match {
      case (ProcessType.am, RunMode.client) =>
        customResourceTypeConfigKey += YARN_AM_RESOURCE_TYPES_PREFIX
      case (ProcessType.driver, RunMode.cluster) =>
        customResourceTypeConfigKey += YARN_DRIVER_RESOURCE_TYPES_PREFIX
      case (ProcessType.executor, _) =>
        customResourceTypeConfigKey += YARN_EXECUTOR_RESOURCE_TYPES_PREFIX
    }

    customResourceTypeConfigKey += rcp.resourceType

    (standardResourceConfigKey, customResourceTypeConfigKey)
  }

  private[spark] def printErrorMessageToBuffer(sb: StringBuilder, str: String): Unit = {
    sb.append(s"$ERROR_PREFIX$str\n")
  }

  private class ResourceConfigProperties(
      val processType: ProcessType,
      val runMode: RunMode = null,
      val resourceType: ResourceType)

  /**
   * Stores all requested custom resources.
   * These resources are retrieved from sparkConf.
   *
   * @param sparkConf
   */
  private class RequestedResources(val sparkConf: SparkConf) {
    val customAMResources: Map[String, String] =
      extractCustomResources(sparkConf, YARN_AM_RESOURCE_TYPES_PREFIX)
    val customDriverResources: Map[String, String] =
      extractCustomResources(sparkConf, YARN_DRIVER_RESOURCE_TYPES_PREFIX)
    val customExecutorResources: Map[String, String] =
      extractCustomResources(sparkConf, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX)

    private def extractCustomResources(
        sparkConf: SparkConf,
        propertyPrefix: String): Map[String, String] = {
      val result: collection.mutable.HashMap[String, String] =
        new collection.mutable.HashMap[String, String]()

      val propertiesWithPrefix: Array[(String, String)] = sparkConf.getAllWithPrefix(propertyPrefix)
      propertiesWithPrefix.foreach { case (k, v) => result.put(propertyPrefix + k, v) }

      result.toMap
    }
  }

}
