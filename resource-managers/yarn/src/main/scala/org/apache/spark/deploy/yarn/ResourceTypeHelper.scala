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

import org.apache.hadoop.yarn.api.records.{Resource, ResourceInformation}

private[yarn] class ResourceTypeHelper {
}

object ResourceTypeHelper {
  def setResourceInfoFromResourceTypes(resourceTypes: Map[String, String],
                                       res: Resource): Resource = {
    if (res == null) {
      throw new IllegalArgumentException("Resource should not be null!")
    }
    resourceTypes.foreach(rt => {
      val resourceName = rt._1
      val (amount, unit) = getAmountAndUnit(rt._2)
      if (unit.length > 0) {
        res.setResourceInformation(resourceName,
          ResourceInformation.newInstance(resourceName, unit, amount))
      } else {
        res.setResourceInformation(resourceName,
          ResourceInformation.newInstance(resourceName, amount))
      }
    })
    res
  }

  def getAmountAndUnit(s: String): (Long, String) = {
    val pattern = "([0-9]+)([A-Za-z]*)".r
    try {
      val pattern(amount, unit) = s
      (amount.toLong, unit)
    } catch {
      case _: MatchError => throw new IllegalArgumentException(
        s"Value of resource type should match pattern $pattern, unmatched value: $s")
    }
  }
}