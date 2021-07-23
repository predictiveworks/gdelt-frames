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

import de.kp.works.gdelt.h3.H3Utils
import de.kp.works.gdelt.model._

import scala.util.Try

package object functions extends Serializable {
    
  def actor_udf(
      countryCodes:Map[String,String], 
      ethnicCodes:Map[String,String], 
      groupCodes:Map[String,String], 
      religionCodes:Map[String,String], 
      typeCodes:Map[String,String]) = 
    udf((row:Row) => {
      /*
       * 0: Code
       * 1: Name
       * 2: Country Code
       * 3: Group Code
       * 4: Ethnic Code
       * 5: Religion1 Code
       * 6: Religion2 Code
       * 7: Type1 Code
       * 8: Type2 Code
       * 9: Type3 Code
       */
      val actorCode = if (row.getAs[String](0) == null) "*" else row.getAs[String](0)
      val actorName = if (row.getAs[String](1) == null) "*" else row.getAs[String](1)
      
      val countryCode = if (row.getAs[String](2) == null) "*" else row.getAs[String](2)
      val countryName = codeToValue(countryCode, countryCodes)
      
      val groupCode = if (row.getAs[String](3) == null) "*" else row.getAs[String](3)
      val groupName = codeToValue(groupCode, groupCodes)
      
      val ethnicCode = if (row.getAs[String](4) == null) "*" else row.getAs[String](4)
      val ethnicName = codeToValue(ethnicCode, ethnicCodes)
      
      val religion1Code = if (row.getAs[String](5) == null) "*" else row.getAs[String](5)
      val religion1Name = codeToValue(religion1Code, religionCodes)
      
      val religion2Code = if (row.getAs[String](6) == null) "*" else row.getAs[String](6)
      val religion2Name = codeToValue(religion2Code, religionCodes)
      
      val type1Code = if (row.getAs[String](7) == null) "*" else row.getAs[String](7)
      val type1Name = codeToValue(type1Code, typeCodes)
      
      val type2Code = if (row.getAs[String](8) == null) "*" else row.getAs[String](8)
      val type2Name = codeToValue(type2Code, typeCodes)
      
      val type3Code = if (row.getAs[String](9) == null) "*" else row.getAs[String](9)
      val type3Name = codeToValue(type3Code, typeCodes)
      
      Actor(
        actorCode,
        actorName,
        countryCode,
        countryName,
        groupCode,
        groupName,
        ethnicCode,
        ethnicName,
        religion1Code,
        religion1Name,
        religion2Code,
        religion2Name,
        type1Code,
        type1Name,
        type2Code,
        type2Name,
        type3Code,
        type3Name)

    })
    
  def action_udf = udf((row:Row) => row
    .toSeq
    .map(value =>
      if (value == null) "*" else value.asInstanceOf[String]))
  
  /**
   * The result describes an enriched hierarchy
   * of event dat can be used for grouping.
   */
  def event_code_udf(eventCodes:Map[String,String]) = 
    udf((row:Row) => {
      /*
       * 0: EventRootCode
       * 1: EventBaseCode
       * 2: EventCode
       */
      val rootCode = if (row.getAs[String]("EventRootCode") == null) "*" else row.getAs[String]("EventRootCode")
      val rootName = eventCodes.getOrElse(rootCode, "*")
      
      val baseCode = if (row.getAs[String]("EventBaseCode") == null) "*" else row.getAs[String]("EventBaseCode")
      val baseName = eventCodes.getOrElse(baseCode, "*")

      val eventCode = if (row.getAs[String]("EventCode") == null) "*" else row.getAs[String]("EventCode")
      val eventName = eventCodes.getOrElse(eventCode, "*")

      Event(
          rootCode,
          rootName,
          baseCode,
          baseName,
          eventCode,
          eventName)
      
    })
    
  def event_id_udf = 
    udf((eventId:String) => eventId.replace("\"","").toInt)
    
  def ethnic_code_udf(ethnicCodes:Map[String,String]) = 
    udf((ethnicCode:String) => codeToValue(ethnicCode, ethnicCodes))
    
  def group_code_udf(groupCodes:Map[String,String]) = 
    udf((groupCode:String) => codeToValue(groupCode, groupCodes))

  def quad_class_udf = 
    udf((quadClass:String) => {
    
      val code = quadClass.trim.toInt
      code match {
        case 1 => "Verbal Cooperation"
        case 2 => "Material Cooperation"
        case 3 => "Verbal Conflict"
        case 4 => "Material Conflict"
        case _ => "*"
      }    
    })
    
  /**
   * This User Defined Function transforms each Geo Point
   * into a H3 index of the provided resolution. This enables
   * an easy grouping of events that refer to the same hexagon.
   */
  def locations_udf(resolution:Int, countryCodes:Map[String,String]) =
    udf((locations:String) => {
      
      if (locations != null) {

        locations.split(";").map(location => {
          
          val blocks = location.split("#")      
          /*
           * 0: GeoType
           * 1: GeoName
           * 2: CountryCode
           * 3: Adm1Code
           * 4: Lat 
           * 5: Lon
           * 6: FeatureId
           */
          val `type` = blocks(0).trim.toInt match {
            case 1 => "Country"
            case 2 => "US State"
            case 3 => "US City"
            case 4 => "World City"
            case 5 => "World State"
            case _ => "*"
          }
          
          val fullName = if (blocks(1) == null) "*" else blocks(1) 
        
          val countryCode = if (blocks(2) == null) "*" else blocks(2)
          val countryName = codeToValue(countryCode, countryCodes)
          
          val adm1Code = if (blocks(3) == null) "*" else blocks(3)
          val adm2Code = "*"
          
          val lat = if (blocks(4) == null) 0D else blocks(4).toDouble
          val lon = if (blocks(5) == null) 0D else blocks(5).toDouble
  
          val coordinate = Seq(lat, lon)
          val index = H3Utils.coordinateToH3(coordinate, resolution)
          
          val featureId = if (blocks(6) == null) "*" else blocks(6)
          Location(
              `type`, 
              fullName, 
              countryCode, 
              countryName, 
              adm1Code, 
              adm2Code, 
              coordinate, 
              resolution, 
              index, 
              featureId)
          
        }).toSeq
        
      } 
      else 
        Seq(Location())

    })
    
  def enhanced_locations_udf(resolution:Int, countryCodes:Map[String,String]) =
    udf((enhancedLocations:String) => {
      
      if (enhancedLocations != null) {

        enhancedLocations.split(";").map(enhancedLocation => {
          
          val blocks = enhancedLocation.split("#")      
          /*
           * 0: GeoType
           * 1: GeoName
           * 2: CountryCode
           * 3: Adm1Code
           * 4: Adm2Code
           * 5: Lat 
           * 6: Lon
           * 7: FeatureId
           * 8: Offset
           */
          val `type` = blocks(0).trim.toInt match {
            case 1 => "Country"
            case 2 => "US State"
            case 3 => "US City"
            case 4 => "World City"
            case 5 => "World State"
            case _ => "*"
          }
          
          val fullName = if (blocks(1) == null) "*" else blocks(1) 
        
          val countryCode = if (blocks(2) == null) "*" else blocks(2)
          val countryName = codeToValue(countryCode, countryCodes)
          
          val adm1Code = if (blocks(3) == null) "*" else blocks(3)
          val adm2Code = if (blocks(4) == null) "*" else blocks(4)
          
          val lat = if (blocks(5) == null) 0D else blocks(5).toDouble
          val lon = if (blocks(6) == null) 0D else blocks(6).toDouble
  
          val coordinate = Seq(lat, lon)
          val index = H3Utils.coordinateToH3(coordinate, resolution)
  
          val featureId = if (blocks(7) == null) "*" else blocks(7)
          val location = Location(
              `type`, 
              fullName, 
              countryCode, 
              countryName, 
              adm1Code, 
              adm2Code, 
              coordinate,
              resolution,
              index,
              featureId)
          
          val offset = blocks(8).toInt
          EnhancedLocation(location, offset)
          
        }).toSeq
        
      }
      else 
        Seq(EnhancedLocation())
    
    })
    
  def location_udf(resolution:Int, countryCodes:Map[String,String], version:String="V1") =
    udf((row:Row) => {
      
      val `type` = row.getAs[String](0).trim.toInt match {
        case 1 => "Country"
        case 2 => "US State"
        case 3 => "US City"
        case 4 => "World City"
        case 5 => "World State"
        case _ => "*"
      }
      
      val fullName = if (row.getAs[String](1) == null) "*" else row.getAs[String](1) 
      
      val countryCode = if (row.getAs[String](2) == null) "*" else row.getAs[String](2)
      val countryName = codeToValue(countryCode, countryCodes)
      
      if (version == "V1") {
        /*
         * 0 : Geo_Type
         * 1 : Geo_Fullname
         * 2 : Geo_CountryCode
         * 3 : Geo_ADM1Code
         * 4 : Geo_Lat
         * 5 : Geo_Long
         * 6 : Geo_FeatureID
         */
        val adm1Code = if (row.getAs[String](3) == null) "*" else row.getAs[String](3)
        val adm2Code = "*"
        
        val lat = if (row.getAs[String](4) == null) 0D else row.getAs[String](4).toDouble
        val lon = if (row.getAs[String](5) == null) 0D else row.getAs[String](5).toDouble
  
        val coordinate = Seq(lat, lon)
        val index = H3Utils.coordinateToH3(coordinate, resolution)

        val featureId = if (row.getAs[String](6) == null) "*" else row.getAs[String](6)
        Location(
            `type`, 
            fullName, 
            countryCode, 
            countryName, 
            adm1Code, 
            adm2Code, 
            coordinate, 
            resolution,
            index,
            featureId)
        
      }
      else {
        /*
         * 0 : Geo_Type
         * 1 : Geo_Fullname
         * 2 : Geo_CountryCode
         * 3 : Geo_ADM1Code
         * 4 : Geo_ADM2Code
         * 5 : Geo_Lat
         * 6 : Geo_Long
         * 7 : Geo_FeatureID
         */
        val adm1Code = if (row.getAs[String](3) == null) "*" else row.getAs[String](3)
        val adm2Code = if (row.getAs[String](4) == null) "*" else row.getAs[String](4)
        
        val lat = if (row.getAs[String](5) == null) 0D else row.getAs[String](5).toDouble
        val lon = if (row.getAs[String](6) == null) 0D else row.getAs[String](6).toDouble
  
        val coordinate = Seq(lat, lon)
        val index = H3Utils.coordinateToH3(coordinate, resolution)

        val featureId = if (row.getAs[String](7) == null) "*" else row.getAs[String](7)
        Location(
            `type`, 
            fullName, 
            countryCode, 
            countryName, 
            adm1Code, 
            adm2Code, 
            coordinate,
            resolution,
            index,
            featureId)
        
      }
    })
    
  def organisations_udf =
    udf((organisations:String) => {
      
      if (organisations != null) {
        organisations.split(";").toSeq
        
      }
      else 
        Seq.empty[String]

    })
    
  def enhanced_organisations_udf =
    udf((enhancedOrganisations:String) => {
      
      if (enhancedOrganisations != null) {
        enhancedOrganisations.split(";").map(enhancedOrganisation => {
          
          val blocks = enhancedOrganisation.split(",")
          
          val organization = if (blocks(0) == null) "*" else blocks(0)
          val offset = blocks(1).toInt

          EnhancedOrganization(organization, offset)
          
        }).toSeq
        
      }
      else 
        Seq(EnhancedOrganization())

      
    })

  def persons_udf = 
    udf((persons:String) => {
      
      if (persons != null) {
        persons.split(";").toSeq
        
      }
      else 
        Seq.empty[String]

    })

  def enhanced_persons_udf = 
    udf((enhancedPersons:String) => {
      
      if (enhancedPersons != null) {
        enhancedPersons.split(";").map(enhancedPerson => {
          
          val blocks = enhancedPerson.split(",")
          
          val person = if (blocks(0) == null) "*" else blocks(0)
          val offset = blocks(1).toInt

          EnhancedPerson(person, offset)
          
        }).toSeq
        
      }
      else 
        Seq(EnhancedPerson())

    })

  def themes_udf = udf((themes:String) => {
  
    if (themes == null) Seq.empty[String]
    else {
      themes.split(";").toSeq
      
    }
  })

  def enhanced_themes_udf = 
    udf((enhancedThemes:String) => {
      
      if (enhancedThemes != null) {
        enhancedThemes.split(";").map(enhancedTheme => {
          
          val blocks = enhancedTheme.split(",")
          
          val theme = if (blocks(0) == null) "*" else blocks(0)
          val offset = blocks(1).toInt

          EnhancedTheme(theme, offset)
          
        }).toSeq
        
      }
      else 
        Seq(EnhancedTheme())
    
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