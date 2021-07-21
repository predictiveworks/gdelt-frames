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

  private val countryCodes  = GDELTModel.countryCodes

  private val eventCodes  = GDELTModel.eventCodes
  private val ethnicCodes = GDELTModel.ethnicCodes
  
  private val groupCodes    = GDELTModel.groupCodes
  private val religionCodes = GDELTModel.religionCodes

  private val typeCodes  = GDELTModel.typeCodes

  def transform(dataset:DataFrame):DataFrame = {
    /*
     * Event enrichment: all event codes are enriched
     * with its associated description and the result
     * is packed into common data structure  
     */
    val eventCols = List("EventCode", "EventBaseCode", "EventRootCode").map(col)
    val eventStruct = struct(eventCols: _*)
    
    val dropCols = List("FractionDate", "EventCode", "EventBaseCode", "EventRootCode", "MonthYear", "Year")
    var enriched = dataset
    
      /** COUNTRY CODE **/
      .withColumn("Actor1_CountryCode", ethnic_code_udf(countryCodes)(col("Actor1_CountryCode")))
      .withColumnRenamed("Actor1_CountryCode", "Actor1_Country")
    
      .withColumn("Actor2_CountryCode", ethnic_code_udf(countryCodes)(col("Actor2_CountryCode")))
      .withColumnRenamed("Actor2_CountryCode", "Actor2_Country")
    
      .withColumn("Actor1_Geo_CountryCode", ethnic_code_udf(countryCodes)(col("Actor1_Geo_CountryCode")))
      .withColumnRenamed("Actor1_Geo_CountryCode", "Actor1_GeoCountry")
    
      .withColumn("Actor2_Geo_CountryCode", ethnic_code_udf(countryCodes)(col("Actor2_Geo_CountryCode")))
      .withColumnRenamed("Actor2_Geo_CountryCode", "Actor2_GeoCountry")
    
      .withColumn("Action_Geo_CountryCode", ethnic_code_udf(countryCodes)(col("Action_Geo_CountryCode")))
      .withColumnRenamed("Action_Geo_CountryCode", "Action_GeoCountry")
      
      /** ETHNIC CODE **/
      .withColumn("Actor1_EthnicCode", ethnic_code_udf(ethnicCodes)(col("Actor1_EthnicCode")))
      .withColumnRenamed("Actor1_EthnicCode", "Actor1_Ethnic")
      
      .withColumn("Actor2_EthnicCode", ethnic_code_udf(ethnicCodes)(col("Actor2_EthnicCode")))
      .withColumnRenamed("Actor2_EthnicCode", "Actor2_Ethnic")
    
      /** EVENT CODE **/
      .withColumn("Event", event_code_udf(eventCodes)(eventStruct))
      
      /** QUAD CLASS **/
      .withColumn("QuadClass",  quad_class_udf(col("QuadClass")))
      
      /** GEO TYPE **/
      .withColumn("Actor1_Geo_Type", geo_type_udf(col("Actor1_Geo_Type")))
      .withColumn("Actor2_Geo_Type", geo_type_udf(col("Actor2_Geo_Type")))
      .withColumn("Action_Geo_Type", geo_type_udf(col("Action_Geo_Type")))
    
      /** GROUP TYPE **/
      .withColumn("Actor1_KnownGroupCode", group_code_udf(groupCodes)(col("Actor1_KnownGroupCode")))
      .withColumnRenamed("Actor1_KnownGroupCode", "Actor1_Group")
      
      .withColumn("Actor2_KnownGroupCode", group_code_udf(groupCodes)(col("Actor2_KnownGroupCode")))
      .withColumnRenamed("Actor2_KnownGroupCode", "Actor2_Group")
       
      /** RELIGION CODE **/
      .withColumn("Actor1_Religion1Code", ethnic_code_udf(religionCodes)(col("Actor1_Religion1Code")))
      .withColumnRenamed("Actor1_Religion1Code", "Actor1_Religion1")
     
      .withColumn("Actor1_Religion2Code", ethnic_code_udf(religionCodes)(col("Actor1_Religion2Code")))
      .withColumnRenamed("Actor1_Religion2Code", "Actor1_Religion2")

      .withColumn("Actor2_Religion1Code", ethnic_code_udf(religionCodes)(col("Actor2_Religion1Code")))
      .withColumnRenamed("Actor2_Religion1Code", "Actor2_Religion1")

      .withColumn("Actor2_Religion2Code", ethnic_code_udf(religionCodes)(col("Actor2_Religion2Code")))
      .withColumnRenamed("Actor2_Religion2Code", "Actor2_Religion2")
        
      /** TYPE **/
      .withColumn("Actor1_Type1Code", group_code_udf(typeCodes)(col("Actor1_Type1Code")))
      .withColumnRenamed("Actor1_Type1Code", "Actor1_Type1")
       
      .withColumn("Actor1_Type2Code", group_code_udf(typeCodes)(col("Actor1_Type2Code")))
      .withColumnRenamed("Actor1_Type2Code", "Actor1_Type2")

      .withColumn("Actor1_Type3Code", group_code_udf(typeCodes)(col("Actor1_Type3Code")))
      .withColumnRenamed("Actor1_Type3Code", "Actor1_Type3")
      
      .withColumn("Actor2_Type1Code", group_code_udf(typeCodes)(col("Actor2_Type1Code")))
      .withColumnRenamed("Actor2_Type1Code", "Actor2_Type1")
       
      .withColumn("Actor2_Type2Code", group_code_udf(typeCodes)(col("Actor2_Type2Code")))
      .withColumnRenamed("Actor2_Type2Code", "Actor2_Type2")

      .withColumn("Actor2_Type3Code", group_code_udf(typeCodes)(col("Actor2_Type3Code")))
      .withColumnRenamed("Actor2_Type3Code", "Actor2_Type3")

      /** DROP **/
      
      .drop(dropCols: _*)
      
    enriched = aggregateActor(enriched, 1)
    enriched = aggregateActorGeo(enriched, 1)
    
    enriched = aggregateActor(enriched, 2)    
    enriched = aggregateActorGeo(enriched, 2)
    
    enriched = aggregateAction(enriched)
    enriched
    
  }
  
  def aggregateActor(dataframe:DataFrame, num:Int):DataFrame = {
   
    val actorCols = List(
        s"Actor${num}_Code", 
        s"Actor${num}_Name", 
        s"Actor${num}_Country", 
        s"Actor${num}_Group", 
        s"Actor${num}_Ethnic", 
        s"Actor${num}_Religion1", 
        s"Actor${num}_Religion2", 
        s"Actor${num}_Type1", 
        s"Actor${num}_Type2", 
        s"Actor${num}_Type3")
        
    val actorStruct = struct(actorCols.map(col): _*)    
    dataframe
      .withColumn(s"Actor${num}", actor_udf(actorStruct))
      .drop(actorCols: _*)

  }
    
  def aggregateActorGeo(dataframe:DataFrame, num:Int):DataFrame = {
   
    val actorCols = List(
        s"Actor${num}_Geo_Type", 
        s"Actor${num}_Geo_Fullname", 
        s"Actor${num}_GeoCountry", 
        s"Actor${num}_Geo_ADM1Code", 
        s"Actor${num}_Geo_Lat", 
        s"Actor${num}_Geo_Long",
        s"Actor${num}_Geo_FeatureID")
        
    val actorStruct = struct(actorCols.map(col): _*)    
    dataframe
      .withColumn(s"Actor${num}_Geo", actor_geo_udf(actorStruct))
      .drop(actorCols: _*)

  }
   
  def aggregateAction(dataframe:DataFrame):DataFrame = {
    
    val actionCols = List(
        "Action_Geo_Type",
        "Action_Geo_Fullname",
        "Action_GeoCountry",
        "Action_Geo_ADM1Code",
        "Action_Geo_Lat",
        "Action_Geo_Long",
        "Action_Geo_FeatureID")
        
    val actionStruct = struct(actionCols.map(col): _*)    
    dataframe
      .withColumn("Action", action_udf(actionStruct))
      .drop(actionCols: _*)
        
  }
   
  
  
}
