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

import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}


class ResourceTypeValidatorSuite extends SparkFunSuite with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  private def getExpectedErrorMessage(res1: String, res2: String): String = {
    s"Error: $res1 and $res2 configs are both present, " +
      s"only one of them is allowed at the same time!"
  }

  test("empty SparkConf should be valid") {
    val sparkConf = new SparkConf()
    ResourceTypeValidator.validateResourceTypes(sparkConf)
  }

  test("just executor memory / cores and driver memory / cores are defined") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "3G")
    sparkConf.set("spark.driver.cores", "4")
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.executor.cores", "2")

    ResourceTypeValidator.validateResourceTypes(sparkConf)
  }


  test("test validation of resource types: Memory defined two ways for executor") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "3G")
    sparkConf.set("spark.driver.cores", "4")
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.executor.cores", "2")

    sparkConf.set("spark.yarn.executor.resource.memory", "30G")

    val thrown = intercept[SparkException] {
      ResourceTypeValidator.validateResourceTypes(sparkConf)
    }
    thrown.getMessage should startWith (getExpectedErrorMessage("spark.executor.memory",
      "spark.yarn.executor.resource.memory"))
  }

  test("test validation of resource types: Cores defined two ways for executor") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "3G")
    sparkConf.set("spark.driver.cores", "4")
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.executor.cores", "2")

    sparkConf.set("spark.yarn.executor.resource.cores", "5")

    val thrown = intercept[SparkException] {
      ResourceTypeValidator.validateResourceTypes(sparkConf)
    }
    thrown.getMessage should startWith (getExpectedErrorMessage("spark.executor.cores",
      "spark.yarn.executor.resource.cores"))
  }

  test("test validation of resource types: Memory defined two ways for driver, client mode") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2G")
    sparkConf.set("spark.driver.cores", "4")
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.executor.cores", "2")

    sparkConf.set("spark.yarn.am.resource.memory", "1G")

    val thrown = intercept[SparkException] {
      ResourceTypeValidator.validateResourceTypes(sparkConf)
    }
    thrown.getMessage should startWith (getExpectedErrorMessage("spark.driver.memory",
      "spark.yarn.am.resource.memory"))
  }

  test("test validation of resource types: Memory defined two ways for driver, cluster mode") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2G")
    sparkConf.set("spark.driver.cores", "4")
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.executor.cores", "2")

    sparkConf.set("spark.yarn.driver.resource.memory", "1G")

    val thrown = intercept[SparkException] {
      ResourceTypeValidator.validateResourceTypes(sparkConf)
    }
    thrown.getMessage should startWith (getExpectedErrorMessage("spark.driver.memory",
      "spark.yarn.driver.resource.memory"))
  }

  test("test validation of resource types: Cores defined two ways for driver, client mode") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2G")
    sparkConf.set("spark.driver.cores", "4")
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.executor.cores", "2")

    sparkConf.set("spark.yarn.am.resource.cores", "3")

    val thrown = intercept[SparkException] {
      ResourceTypeValidator.validateResourceTypes(sparkConf)
    }
    thrown.getMessage should startWith (getExpectedErrorMessage("spark.driver.cores",
      "spark.yarn.am.resource.cores"))
  }

  test("test validation of resource types: Cores defined two ways for driver, cluster mode") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2G")
    sparkConf.set("spark.driver.cores", "4")
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.executor.cores", "2")

    sparkConf.set("spark.yarn.driver.resource.cores", "1G")

    val thrown = intercept[SparkException] {
      ResourceTypeValidator.validateResourceTypes(sparkConf)
    }
    thrown.getMessage should startWith (getExpectedErrorMessage("spark.driver.cores",
      "spark.yarn.driver.resource.cores"))
  }

  test("test validation of resource types: various duplicated definitions") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2G")
    sparkConf.set("spark.driver.cores", "2")
    sparkConf.set("spark.executor.memory", "2G")
    sparkConf.set("spark.executor.cores", "4")

    sparkConf.set("spark.yarn.executor.resource.memory", "3G")
    sparkConf.set("spark.yarn.am.resource.memory", "2G")
    sparkConf.set("spark.yarn.driver.resource.memory", "2G")

    val thrown = intercept[SparkException] {
      ResourceTypeValidator.validateResourceTypes(sparkConf)
    }
    thrown.getMessage should (
      include(getExpectedErrorMessage("spark.executor.memory",
        "spark.yarn.executor.resource.memory")) and
        include(getExpectedErrorMessage("spark.driver.memory",
          "spark.yarn.am.resource.memory")) and
        include(getExpectedErrorMessage("spark.driver.memory",
          "spark.yarn.driver.resource.memory"))
      )
  }

}
