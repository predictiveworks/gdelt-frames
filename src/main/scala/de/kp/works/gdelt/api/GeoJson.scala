package de.kp.works.gdelt.api
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
import com.google.gson._
import scala.collection.JavaConverters._
import scala.collection.mutable
import de.kp.works.gdelt.Mention
import de.kp.works.gdelt.MentionHtml

trait GeoJson {
  
  protected def extractHtml(html:String):Seq[MentionHtml] = {
    /*
     * A list of <a ...> anchors separated by <br>
     */
    val parsed = org.jsoup.Jsoup.parse(html)
    val anchors = parsed.select("a")
    
    val features = for (anchor <- anchors.asScala) yield {
      
      val href = anchor.attr("href")
      val title = anchor.attr("title")
      
      MentionHtml(title, href)
      
    }
     
    features
    
  }
  
  protected def geoJsonToMentions(geojson:JsonObject):Seq[Mention] = {
    
    val jFeatures = geojson.get("features").getAsJsonArray
    /*
     * A feature contains a `properties` and a `geometry` 
     * object that will be extracted. Note, GeoSJON coordinates
     * are described in Lon-Lat order.
     */
    val features = mutable.ArrayBuffer.empty[Mention]
    (0 until jFeatures.size).foreach(i => {
      
      val jFeature = jFeatures.get(i).getAsJsonObject
      
      /**** PROPERTIES ****/
      
      val jProperties = jFeature.get("properties").getAsJsonObject
      
      val name  = jProperties.get("name").getAsString
      val count = jProperties.get("count").getAsInt
      
      val shareimage = jProperties.get("shareimage").getAsString
      val html = extractHtml(jProperties.get("html").getAsString)
      
      /**** GEOMETRY ****/
      
      val jGeometry   = jFeature.get("geometry").getAsJsonObject
      val jType = jGeometry.get("type").getAsString
      /*
       * The current implementation supports POINT geometry only
       */
      if (jType != "Point") {
        throw new Exception(s"Geometries other than `Point` are not supported yet.")
      }
      
      val jCoordinates = jGeometry.get("coordinates").getAsJsonArray
      val latlon = Array.fill[Double](2)(0D)
      
      latlon(0) = jCoordinates.get(1).getAsDouble
      latlon(1) = jCoordinates.get(0).getAsDouble
      
      val feature = 
        Mention(name = name, count = count, shareimage = shareimage, html = html, latlon = latlon)
      
      features += feature
      
    })

    features
    
  }
  
  protected def extractGeoJson(html:org.jsoup.nodes.Document):JsonObject = {

    var json:JsonElement = null
    
    val body = html.body()
    
    val scripts = body.select("script")
    for (script <- scripts.asScala) {
      /*
       * Distinguish between script tags with a certain source
       * and others
       */
      val rawdata = script.data()
      if (rawdata.isEmpty == false) {
        /*
         * Extrac GeoJSON from one the non-empty
         * script tags
         */
        if (rawdata.trim.startsWith("var pointmap =")) {
          /*
           * Extract from variable declaration; this implies
           * that the lasr characters can be a ";"
           */
          var rawstr = rawdata.trim.replace("var pointmap =", "").trim
          rawstr = rawstr.substring(0, rawstr.length - 1)

          json = try {
            JsonParser.parseString(rawstr)
            
          } catch {
            case t:Throwable => {
              t.printStackTrace(); null
            }
          }
        }
      }
    }

    json.getAsJsonObject
    
  }

}