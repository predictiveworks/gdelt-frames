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

 case class Content(
  url        : String,
  title      : Option[String] = None,
  content    : Option[String] = None,
  description: Option[String] = None,
  keywords   : Array[String] = Array.empty[String],
  publishDate: Option[Date] = None,
  imageURL   : Option[String] = None,
  imageBase64: Option[String] = None)

case class EventCode(
  code : String,
  value: String
)

case class MentionHtml(
  title:String, 
  uri  :String)

case class Mention(
  name      :String, 
  count     :Int, 
  shareimage:String, 
  latlon    :Seq[Double], 
  html      :Seq[MentionHtml])

object GDELTModel extends Serializable {

  val countryCodes:Map[String,String]  = loadCountryCodes

  val eventCodes:Map[String,String]  = loadEventCodes
  val ethnicCodes:Map[String,String] = loadEthnicCodes

  val groupCodes:Map[String,String]    = loadGroupCodes
  val religionCodes:Map[String,String] = loadReligionCodes

  val typeCodes:Map[String,String] = loadTypeCodes
  
  /**
   * A helper method to load country specific
   * geo information from a resource file.
   * 
   * It is used to enrich the country specific
   * columns of a GDELT event.
   */
  def loadCountryCodes:Map[String,String] = {
    
    val is = this.getClass.getResourceAsStream("cameoCountry.txt")
    toMap(is)

  }

  def loadEthnicCodes:Map[String,String] = {

    val is = this.getClass.getResourceAsStream("cameoEthnic.txt")
    toMap(is)

  }
  
  def loadEventCodes:Map[String,String] = {

    val is = this.getClass.getResourceAsStream("cameoEvent.txt")
    toMap(is)
    
  }

  def loadGroupCodes:Map[String,String] = {

    val is = this.getClass.getResourceAsStream("cameoGroup.txt")
    toMap(is)

  }
  
  def loadReligionCodes:Map[String,String] = {

    val is = this.getClass.getResourceAsStream("cameoReligion.txt")
    toMap(is)
    
  }
  
  def loadTypeCodes:Map[String,String] = {

    val is = this.getClass.getResourceAsStream("cameoType.txt")
    toMap(is)
    
  }

  def toMap(is:java.io.InputStream):Map[String,String] = {
    
    scala.io.Source.fromInputStream(is, "UTF-8").getLines()
      /* Ignore header */
      .toSeq.drop(1)
      .map(line => {
        val tokens = line.split("\t")
        (tokens(0), tokens(1).toLowerCase)
      })
      .toMap
    
  }
}