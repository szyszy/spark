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

import java.util.{List => JList, Map => JMap}

import org.apache.hadoop.yarn.api.records.ResourceTypeInfo
import org.apache.hadoop.yarn.util.resource.ResourceUtils
import org.junit.Assert

import scala.collection.JavaConverters._

import scala.collection.mutable.ListBuffer

object TestYarnResourceTypeHelper {
  def initializeResourceTypes(resourceTypes: List[String]) = {
    val allResourceTypes = new ListBuffer[ResourceTypeInfo]
    val defaultResourceTypes = List(
      ResourceTypeInfo.newInstance("memory-mb"),
      ResourceTypeInfo.newInstance("vcores"))
    val customResourceTypes = resourceTypes.map(rt => ResourceTypeInfo.newInstance(rt))

    allResourceTypes.++=(defaultResourceTypes)
    allResourceTypes.++=(customResourceTypes)

    try {
      ResourceUtils.reinitializeResources(allResourceTypes.asJava)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Assert.fail("resource map initialization failed")
    }
  }
}