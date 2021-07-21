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
import org.apache.spark.sql._

import org.apache.spark.sql.functions._
import de.kp.works.gdelt.functions._

class Enricher {

  private val eventCodes  = GDELTModel.eventCodes
  private val ethnicCodes = GDELTModel.ethnicCodes
  
  private val religionCodes = GDELTModel.religionCodes

  def transform(dataset:DataFrame):DataFrame = {
    /*
     * Event enrichment: all event codes are enriched
     * with its associated description and the result
     * is packed into common data structure  
     */
    val eventCols = List("EventCode", "EventBaseCode", "EventRootCode").map(col)
    val eventStruct = struct(eventCols: _*)
    
    val dropCols = List("EventCode", "EventBaseCode", "EventRootCode")
    
    val enriched = dataset
    
      /** ETHNIC CODE **/
      .withColumn("Actor1_EthnicCode", ethnic_code_udf(ethnicCodes)(col("Actor1_EthnicCode")))
      .withColumn("Actor2_EthnicCode", ethnic_code_udf(ethnicCodes)(col("Actor2_EthnicCode")))
    
      /** EVENT CODE **/
      .withColumn("Event", event_code_udf(eventCodes)(eventStruct))
      
      /** QUAD CLASS **/
      .withColumn("QuadClass",  quad_class_udf(col("QuadClass")))
      
      /** GEO TYPE **/
      .withColumn("Actor1_Geo_Type", geo_type_udf(col("Actor1_Geo_Type")))
      .withColumn("Actor2_Geo_Type", geo_type_udf(col("Actor2_Geo_Type")))
      .withColumn("Action_Geo_Type", geo_type_udf(col("Action_Geo_Type")))
    
      /** RELIGION CODE **/
      .withColumn("Actor1_Religion1Code", ethnic_code_udf(religionCodes)(col("Actor1_Religion1Code")))
      .withColumn("Actor1_Religion2Code", ethnic_code_udf(religionCodes)(col("Actor1_Religion2Code")))
      .withColumn("Actor2_Religion1Code", ethnic_code_udf(religionCodes)(col("Actor2_Religion1Code")))
      .withColumn("Actor2_Religion2Code", ethnic_code_udf(religionCodes)(col("Actor2_Religion2Code")))
        
      /** DROP **/
      
      .drop(dropCols: _*)
      
    enriched
    
  }
}
