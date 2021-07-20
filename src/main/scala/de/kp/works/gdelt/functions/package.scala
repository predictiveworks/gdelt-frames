package de.kp.works.gdelt
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

package object functions extends Serializable {
  /**
   * The result describes an enriched hierarchy
   * of event dat can be used for grouping.
   */
  def event_code_udf(eventCodes:Map[String,String]) = 
    udf((row:Row) => {
      
      val eventCode = row.getAs[String]("EventCode")
      val eventBaseCode = row.getAs[String]("EventBaseCode")
      
      val eventRootCode = row.getAs[String]("EventRootCode")

      Seq(
        EventCode(eventRootCode, eventCodes.getOrElse(eventRootCode, "*")),
        EventCode(eventBaseCode, eventCodes.getOrElse(eventBaseCode, "*")),
        EventCode(eventCode, eventCodes.getOrElse(eventCode, "*"))
      )
      
    })
    
  def ethnic_code_udf(ethnicCodes:Map[String,String]) = 
    udf((ethnicCode:String) => {
  
      if (ethnicCode == null)
        "unknown"
        
      else
        ethnicCodes.getOrElse(ethnicCode, "unknown")
        
    })

  def geo_type_udf = 
    udf((geoType:String) => {
      
      val code = geoType.trim.toInt
      code match {
        case 1 => "COUNTRY"
        case 2 => "USSTATE"
        case 3 => "USCITY"
        case 4 => "WORLDCITY"
        case 5 => "WORLDSTATE"
        case _ => "UNKNOWN"
      }        
    })

  def quad_class_udf = 
    udf((quadClass:String) => {
    
      val code = quadClass.trim.toInt
      code match {
        case 1 => "VERBAL_COOPERATION"
        case 2 => "MATERIAL_COOPERATION"
        case 3 => "VERBAL_CONFLICT"
        case 4 => "MATERIAL_CONFLICT"
        case _ => "UNKNOWN"
      }    
    })
    
}