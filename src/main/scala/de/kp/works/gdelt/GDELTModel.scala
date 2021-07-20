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


case class CountryCode(
  iso       : Option[String] = None,
  iso3      : Option[String] = None,
  isoNumeric: Option[String] = None,
  fips      : Option[String] = None,
  country   : Option[String] = None)

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

  val eventCodes:Map[String,String]  = loadEventCodes
  val ethnicCodes:Map[String,String] = loadEthnicCodes
  
  def T[A](r: () => A): Option[A] = {
    try {
      val x: A = r.apply()
      x match {
        case _ if(x == null) => None
        case p: String => if (p.trim.length == 0) None else Some(p.trim.asInstanceOf[A])
        case _ => Some(x)
      }
    } catch {
      case _: Throwable => None
    }
  }
  /**
   * A helper method to load country specific
   * geo information from a resource file.
   * 
   * It is used to enrich the country specific
   * columns of a GDELT event.
   */
  def loadCountryCodes = {
    
    val is = this.getClass.getResourceAsStream("countryInfo.txt")
    scala.io.Source.fromInputStream(is, "UTF-8").getLines()
      .toSeq
      /* Ignore header */
      .drop(1).map(line => {
        val tokens = line.split("\t")
          CountryCode(
            iso        = T(() => tokens(0)),
            iso3       = T(() => tokens(1)),
            isoNumeric = T(() => tokens(2)),
            fips       = T(() => tokens(3)),
            country    = T(() => tokens(4).toLowerCase())
          )
      })
  }

  def loadEthnicCodes:Map[String,String] = {

    val is = this.getClass.getResourceAsStream("cameoEthnic.txt")
    
    scala.io.Source.fromInputStream(is, "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      (tokens(0), tokens(1).toLowerCase)
    })
    .toMap
  }
  
  def loadEventCodes:Map[String,String] = {

    val is = this.getClass.getResourceAsStream("cameoEvent.txt")
    
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