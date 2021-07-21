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
import java.sql.Date

import com.gravity.goose.Goose
import org.apache.commons.lang.StringUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import scala.util.Try

package object functions extends Serializable {
    
  def actor_udf = udf((row:Row) => row.toSeq.map(_.asInstanceOf[String]))
    
  def actor_geo_udf = udf((row:Row) => row
    .toSeq
    .map(value =>
      if (value == null) "*" else value.asInstanceOf[String]))
    
  def action_udf = udf((row:Row) => row
    .toSeq
    .map(value =>
      if (value == null) "*" else value.asInstanceOf[String]))
  
  def country_code_udf(countryCodes:Map[String,String]) = 
    udf((countryCode:String) => {
      val countryValue = codeToValue(countryCode, countryCodes)
      Seq(countryCode, countryValue)
     })
  
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
    
  def event_id_udf = 
    udf((eventId:String) => eventId.replace("\"","").toInt)
    
  def ethnic_code_udf(ethnicCodes:Map[String,String]) = 
    udf((ethnicCode:String) => codeToValue(ethnicCode, ethnicCodes))

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
    
  def group_code_udf(groupCodes:Map[String,String]) = 
    udf((groupCode:String) => codeToValue(groupCode, groupCodes))

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
    
  def religion_code_udf(religionCodes:Map[String,String]) = 
    udf((religionCode:String) => codeToValue(religionCode, religionCodes))
    
  def themes_udf = udf((themes:String) => {
  
    if (themes == null) Seq("*")
    else {
      themes.split(";")
        .filter(theme => theme.nonEmpty && theme != null)
        .toSeq
      
    }
  })

  def type_code_udf(typeCodes:Map[String,String]) = 
    udf((typeCode:String) => codeToValue(typeCode, typeCodes))

  def codeToValue(code:String, mapping:Map[String,String]):String = {
      if (code == null)"*" else mapping.getOrElse(code, "*")        
  }
  
  def scrapeContent(iterator: Iterator[String], goose: Goose): Iterator[Content] = {
    
    iterator.map(url => {
      
      Try {
        
        val article = goose.extractContent(url)
        
        val content = if (StringUtils.isNotEmpty(article.cleanedArticleText)) Some(article.cleanedArticleText.replaceAll("\\n+", "\n")) else None
        val keywords = if (StringUtils.isNotEmpty(article.metaKeywords)) article.metaKeywords.split(",").map(_.trim.toUpperCase) else Array.empty[String]
        
        val desc =  if (StringUtils.isNotEmpty(article.metaDescription)) Some(article.metaDescription) else None
        val title = if (StringUtils.isNotEmpty(article.title)) Some(article.title) else None
        
        val publishDate = if (article.publishDate != null) Some(new Date(article.publishDate.getTime)) else None
        
        val imageUrl = if (article.topImage != null && StringUtils.isNotEmpty(article.topImage.imageSrc)) Some(article.topImage.imageSrc) else None
        val imageBase64 = if (article.topImage != null && StringUtils.isNotEmpty(article.topImage.imageBase64)) Some(article.topImage.imageBase64) else None
        
        Content(
          url         = url,
          title       = title,
          content     = content,
          description = desc,
          keywords    = keywords,
          publishDate = publishDate,
          imageURL    = imageUrl,
          imageBase64 =imageBase64)
        
      } getOrElse Content(url)
      
    })
  }
    
}