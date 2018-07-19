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

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.yarn.api.records.Resource
import org.junit.Assert

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

object TestYarnResourceTypeHelper extends Logging {
  def initializeResourceTypes(resourceTypes: List[String]): Unit = {
    if (!ResourceTypeHelper.isYarnResourceTypesAvailable()) {
      throw new IllegalStateException("initializeResourceTypes() should not be invoked " +
        "since YARN resource types is not available because of old Hadoop version!" )
    }

    val allResourceTypes = new ListBuffer[AnyRef]
    val defaultResourceTypes = List(
      createResourceTypeInfo("memory-mb"),
      createResourceTypeInfo("vcores"))
    val customResourceTypes = resourceTypes.map(rt => createResourceTypeInfo(rt))

    allResourceTypes.++=(defaultResourceTypes)
    allResourceTypes.++=(customResourceTypes)

    reinitializeResources(allResourceTypes)
  }

  private def createResourceTypeInfo(resourceName: String): AnyRef = {
    val resTypeInfoClass = Utils.classForName("org.apache.hadoop.yarn.api.records.ResourceTypeInfo")
    val resTypeInfoNewInstanceMethod = resTypeInfoClass.getMethod("newInstance", classOf[String])
    resTypeInfoNewInstanceMethod.invoke(null, resourceName)
  }

  private def reinitializeResources(resourceTypes: ListBuffer[AnyRef]): Unit = {
    val resourceUtilsClass =
      Utils.classForName("org.apache.hadoop.yarn.util.resource.ResourceUtils")
    val reinitializeResourcesMethod = resourceUtilsClass.getMethod("reinitializeResources",
      classOf[java.util.List[AnyRef]])
    try {
      reinitializeResourcesMethod.invoke(null, resourceTypes.asJava)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Assert.fail("resource map initialization failed")
    }
  }

  def getResourceTypeValue(res: Resource, name: String): AnyRef = {
    val resourceInformation: AnyRef = getResourceInformation(res, name)
    invokeMethod(resourceInformation, "getValue")
  }

  def getResourceInformationByName(res: Resource, nameParam: String): ResourceInformation = {
    val resourceInformation: AnyRef = getResourceInformation(res, nameParam)
    val name = invokeMethod(resourceInformation, "getName")
    val value = invokeMethod(resourceInformation, "getValue")
    val units = invokeMethod(resourceInformation, "getUnits")
    new ResourceInformation(
      name.asInstanceOf[String],
      value.asInstanceOf[Long],
      units.asInstanceOf[String])
  }

  private def getResourceInformation(res: Resource, name: String) = {
    if (!ResourceTypeHelper.isYarnResourceTypesAvailable()) {
      throw new IllegalStateException("assertResourceTypeValue() should not be invoked " +
        "since yarn resource types is not available because of old Hadoop version!")
    }

    val getResourceInformationMethod = res.getClass.getMethod("getResourceInformation",
      classOf[String])
    val resourceInformation = getResourceInformationMethod.invoke(res, name)
    resourceInformation
  }

  private def invokeMethod(resourceInformation: AnyRef, methodName: String): AnyRef = {
    val getValueMethod = resourceInformation.getClass.getMethod(methodName)
    getValueMethod.invoke(resourceInformation)
  }

  class ResourceInformation(val name: String, val value: Long, val units: String) {
  }
}
