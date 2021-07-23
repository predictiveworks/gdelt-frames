package de.kp.works.gdelt.h3
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import org.apache.spark.sql.functions._
import com.uber.h3core._

/**
 * Make H3 serializable to enable usage
 * within Apache Spark SQL UDFs
 */
object H3 extends Serializable {
  val instance:H3Core = H3Core.newInstance()
}

object H3Utils extends Serializable {

  /**
   * Indexes the location at the specified resolution,
   * returning the index of the cell containing this
   * location.
   */
  def coordinate_to_H3(resolution:Int) =
    udf((coordinate:Seq[Double]) => coordinateToH3(coordinate, resolution))

  def coordinateToH3(coordinate:Seq[Double], resolution:Int): Long = {
    val (lon, lat) = (coordinate.head, coordinate.last)
    H3.instance.geoToH3(lat, lon, resolution)
  }

}