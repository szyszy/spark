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
   * is defined twice, with its config property and along with a custom resource definition.<br>
   *
   * Example of an invalid config:<br>
   * - spark.driver.memory=1g<br>
   * - spark.yarn.driver.resource.memory=2g<br>
   *
   * This method iterates checks all possible variations of memory / cores definitions,
   * @see [[POSSIBLE_RESOURCE_DEFINITIONS]]
   * <br>
   * Please note that if multiple resources are defined twice,
   * the error messages will be concatenated.<br>
   * Example of such a config:<br>
   * - spark.driver.memory=1g<br>
   * - spark.yarn.driver.resource.memory=2g<br>
   * - spark.executor.cores=4<br>
   * - spark.yarn.executor.resource.cores=2<br>
   * Then the following two error messages will be printed:<br>
   * - "spark.driver.memory and spark.yarn.driver.resource.memory configs are both present,
   * just spark.driver.memory should be used!"<br>
   * - "spark.executor.cores and spark.yarn.executor.resource.cores configs are both present,
   * just spark.executor.cores should be used!"<br>
   *
   * @param sparkConf
   */
  def validateResources(sparkConf: SparkConf): Unit = {
    val requestedResources = new RequestedResources(sparkConf)
    val sb = new mutable.StringBuilder()
    POSSIBLE_RESOURCE_DEFINITIONS.foreach { rcp =>
      val customResources: Map[String, String] = getCustomResourceValue(requestedResources, rcp)
      val standardResourceValue: String = getStandardResourceValue(requestedResources, rcp)
      val (standardResourceConfigKey: String, customResourceConfigKey: String) =
        getResourceConfigKeys(rcp)

      val errorMessage =
        if (standardResourceValue != null && customResources.contains(customResourceConfigKey)) {
          s"$standardResourceConfigKey and $customResourceConfigKey" +
              s" configs are both present, just $standardResourceConfigKey should be used!"
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
   * Returns the requested standard resource (memory or cores),
   * based on the ResourceConfigProperties argument.
   * Example: If the ResourceConfigProperties object has driver as process type
   * and memory as resource type, then the memory value of all the requested resources
   * will be returned.
   * @param requestedResources
   * @param rcp
   * @return
   */
  private def getStandardResourceValue(
      requestedResources: RequestedResources,
      rcp: ResourceConfigProperties): String = {
    var resourceObject: String = null
    (rcp.processType, rcp.resourceType) match {
      case (ProcessType.driver, ResourceType.cores) =>
        resourceObject = requestedResources.driverCores
      case (ProcessType.driver, ResourceType.memory) =>
        resourceObject = requestedResources.driverMemory
      case (ProcessType.executor, ResourceType.cores) =>
        resourceObject = requestedResources.executorCores
      case (ProcessType.executor, ResourceType.memory) =>
        resourceObject = requestedResources.executorMemory
      case (ProcessType.am, ResourceType.memory) =>
        resourceObject = requestedResources.amMemory
      case (ProcessType.am, ResourceType.cores) =>
        resourceObject = requestedResources.amCores
    }
    resourceObject
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
   * Stores all types of requested resources.
   * The resources are retrieved from sparkConf.
   *
   * @param sparkConf
   */
  private class RequestedResources(val sparkConf: SparkConf) {
    val driverMemory: String = sparkConf.getOption(DRIVER_MEMORY.key).orNull
    val driverCores: String = sparkConf.getOption(DRIVER_CORES.key).orNull
    val executorMemory: String = sparkConf.getOption(EXECUTOR_MEMORY.key).orNull
    val executorCores: String = sparkConf.getOption(EXECUTOR_CORES.key).orNull
    val amMemory: String = sparkConf.getOption(AM_MEMORY.key).orNull
    val amCores: String = sparkConf.getOption(AM_CORES.key).orNull
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
