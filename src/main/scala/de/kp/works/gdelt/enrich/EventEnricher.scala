package de.kp.works.gdelt.enrich
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

class EventEnricher extends BaseEnricher[EventEnricher] {
  
  def transform(dataset:DataFrame):DataFrame = {
    
    val dropCols = List("FractionDate", "MonthYear", "Year")
    var enriched = dataset
      
      /** QUAD CLASS **/
      .withColumn("QuadClass",  quad_class_udf(col("QuadClass")))

      /** DROP **/
      
      .drop(dropCols: _*)
    
    /* Aggregate events */
    enriched = aggregateEvent(enriched)
    
    /* Aggregate actors */
    enriched = aggregateActor(enriched, 1)
    enriched = aggregateActor(enriched, 2)
      
    /*
     * Aggregate and enrich Geo information
     */
    enriched = aggregateActorGeo(enriched, 1)
    enriched = aggregateActorGeo(enriched, 2)
    
    enriched = aggregateActionGeo(enriched)
    enriched
    
  }
  
  def aggregateActor(dataframe:DataFrame, num:Int):DataFrame = {
   
    val actorCols = List(
        s"Actor${num}_Code", 
        s"Actor${num}_Name", 
        s"Actor${num}_CountryCode", 
        s"Actor${num}_KnownGroupCode", 
        s"Actor${num}_EthnicCode", 
        s"Actor${num}_Religion1Code", 
        s"Actor${num}_Religion2Code", 
        s"Actor${num}_Type1Code", 
        s"Actor${num}_Type2Code", 
        s"Actor${num}_Type3Code")
        
    val actor = actor_udf(cameoCountryCodes, ethnicCodes, groupCodes, religionCodes, typeCodes)
      
    val actorStruct = struct(actorCols.map(col): _*)    
    dataframe
      .withColumn(s"Actor${num}", actor(actorStruct))
      .drop(actorCols: _*)

  }
   
  def aggregateActorGeo(dataframe:DataFrame, num:Int):DataFrame = {
    
    val actorCols = if (version == "V1")
      List(
        s"Actor${num}_Geo_Type", 
        s"Actor${num}_Geo_Fullname", 
        s"Actor${num}_Geo_CountryCode", 
        s"Actor${num}_Geo_ADM1Code", 
        s"Actor${num}_Geo_Lat", 
        s"Actor${num}_Geo_Long",
        s"Actor${num}_Geo_FeatureID")
    else
       List(
        s"Actor${num}_Geo_Type", 
        s"Actor${num}_Geo_Fullname", 
        s"Actor${num}_Geo_CountryCode", 
        s"Actor${num}_Geo_ADM1Code", 
        s"Actor${num}_Geo_ADM2Code", 
        s"Actor${num}_Geo_Lat", 
        s"Actor${num}_Geo_Long",
        s"Actor${num}_Geo_FeatureID")
             
    val actorStruct = struct(actorCols.map(col): _*)    
    dataframe
      .withColumn(s"Actor${num}Geo", location_udf(resolution, countryCodes)(actorStruct))
      .drop(actorCols: _*)
   
  }
  
  def aggregateActionGeo(dataframe:DataFrame):DataFrame = {
    
    val actionCols = if (version == "V1")
      List(
        "Action_Geo_Type",
        "Action_Geo_Fullname",
        "Action_Geo_CountryCode",
        "Action_Geo_ADM1Code",
        "Action_Geo_Lat",
        "Action_Geo_Long",
        "Action_Geo_FeatureID")
    else
       List(
        "Action_Geo_Type",
        "Action_Geo_Fullname",
        "Action_Geo_CountryCode",
        "Action_Geo_ADM1Code",
        "Action_Geo_ADM2Code",
        "Action_Geo_Lat",
        "Action_Geo_Long",
        "Action_Geo_FeatureID")
             
    val actionStruct = struct(actionCols.map(col): _*)    
    dataframe
      .withColumn("ActionGeo", location_udf(resolution, countryCodes)(actionStruct))
      .drop(actionCols: _*)
        
  }
   
  def aggregateEvent(dataframe:DataFrame):DataFrame = {

    val eventCols = List("EventCode", "EventBaseCode", "EventRootCode")
    val eventStruct = struct(eventCols.map(col): _*)
    
    val dropCols = List("EventCode", "EventBaseCode", "EventRootCode")
    dataframe
      .withColumn("Event", event_code_udf(eventCodes)(eventStruct))
      .drop(eventCols: _*)

  }
  
  
}
