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
import org.apache.hadoop.yarn.api.records.{Resource, ResourceTypeInfo}
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.yarn.util.resource.ResourceUtils
import org.apache.spark.SparkFunSuite
import org.junit.Assert
import org.scalatest.Matchers

import scala.collection.mutable.ListBuffer


class ResourceTypeHelperSuite extends SparkFunSuite with Matchers {

  private val CUSTOM_RES_1 = "custom-resource-type-1"
  private val CUSTOM_RES_2 = "custom-resource-type-2"

  test("resource type value does not match pattern") {
    TestYarnResourceTypeHelper.initializeResourceTypes(List())

    val resourceTypes = Map(CUSTOM_RES_1 -> "**@#")

    val thrown = intercept[IllegalArgumentException] {
      ResourceTypeHelper.setResourceInfoFromResourceTypes(resourceTypes, createAResource)
    }
    thrown.getMessage should equal ("Value of resource type should match pattern " +
      "([0-9]+)([A-Za-z]*), unmatched value: **@#")
  }

  test("resource type just unit defined") {
    TestYarnResourceTypeHelper.initializeResourceTypes(List())

    val resourceTypes = Map(CUSTOM_RES_1 -> "m")

    val thrown = intercept[IllegalArgumentException] {
      ResourceTypeHelper.setResourceInfoFromResourceTypes(resourceTypes, createAResource)
    }
    thrown.getMessage should equal ("Value of resource type should match pattern " +
      "([0-9]+)([A-Za-z]*), unmatched value: m")
  }

  test("resource type with null value should not be allowed") {
    TestYarnResourceTypeHelper.initializeResourceTypes(List())

    val resourceTypes = Map(CUSTOM_RES_1 -> "123")

    val thrown = intercept[IllegalArgumentException] {
      ResourceTypeHelper.setResourceInfoFromResourceTypes(resourceTypes, null)
    }
    thrown.getMessage should equal ("Resource should not be null!")
  }

  test("resource type with valid value and invalid unit") {
    TestYarnResourceTypeHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "123ppp")
    val resource = createAResource

    val thrown = intercept[IllegalArgumentException] {
      ResourceTypeHelper.setResourceInfoFromResourceTypes(resourceTypes, resource)
    }
    thrown.getMessage should fullyMatch regex
      """Unknown unit 'ppp'\. Known units are \[.*\]"""
  }

  test("resource type with valid value and without unit") {
    TestYarnResourceTypeHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "123")
    val resource = createAResource

    ResourceTypeHelper.setResourceInfoFromResourceTypes(resourceTypes, resource)
    val customResource = resource.getResourceInformation(CUSTOM_RES_1)
    customResource.getName should equal (CUSTOM_RES_1)
    customResource.getValue should be (123)
    customResource.getUnits should be ("")
  }

  test("resource type with valid value and unit") {
    TestYarnResourceTypeHelper.initializeResourceTypes(List(CUSTOM_RES_1))

    val resourceTypes = Map(CUSTOM_RES_1 -> "123m")
    val resource = createAResource

    ResourceTypeHelper.setResourceInfoFromResourceTypes(resourceTypes, resource)
    val customResource = resource.getResourceInformation(CUSTOM_RES_1)
    customResource.getName should equal (CUSTOM_RES_1)
    customResource.getValue should be (123)
    customResource.getUnits should be ("m")
  }

  test("two resource types with valid values and units") {
    TestYarnResourceTypeHelper.initializeResourceTypes(List(CUSTOM_RES_1, CUSTOM_RES_2))

    val resourceTypes = Map(
      CUSTOM_RES_1 -> "123m",
      CUSTOM_RES_2 -> "10G"
    )
    val resource = createAResource

    ResourceTypeHelper.setResourceInfoFromResourceTypes(resourceTypes, resource)
    val customResource1 = resource.getResourceInformation(CUSTOM_RES_1)
    customResource1.getName should equal (CUSTOM_RES_1)
    customResource1.getValue should be (123)
    customResource1.getUnits should be ("m")

    val customResource2 = resource.getResourceInformation(CUSTOM_RES_2)
    customResource2.getName should equal (CUSTOM_RES_2)
    customResource2.getValue should be (10)
    customResource2.getUnits should be ("G")
  }

  private def createAResource = {
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemorySize(512)
    resource.setVirtualCores(2)
    resource
  }


}
