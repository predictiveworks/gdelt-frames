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

import de.kp.works.gdelt.Mention

class GeoApi extends BaseApi[GeoApi] with GeoJson {
  /**
   * https://blog.gdeltproject.org/gdelt-geo-2-0-api-debuts/
   */
  private val base = "https://api.gdeltproject.org/api/v2/geo/geo"
  
  def mentionsByDomain(domain:String):Seq[Mention] = {
        
    val endpoint = s"${base}?query=domain:${domain}&format=GeoJSON"
    val geojson = getJson(endpoint).getAsJsonObject
    
    geoJsonToMentions(geojson)
    
  }
  
  def mentionsByTerm(term:String):Seq[Mention] = {
        
    val endpoint = s"${base}?query=${term}&format=GeoJSON"
    val geojson = getJson(endpoint).getAsJsonObject
    
    geoJsonToMentions(geojson)
    
  }
  
  /*
   * Point-level map of all locations mentioned near `query. 
   * This is the most basic kind of map you can make and simply 
   * tallies up the locations mentioned most frequently over 
   * the last 24 hours in close proximity to the `query`. 
   */
  def getMentionsHtml(term:String):Seq[Mention] = {
        
    val endpoint = s"${base}?query=${term}"

    val html = org.jsoup.Jsoup.connect(endpoint).get
    val geojson = extractGeoJson(html)
    
    geoJsonToMentions(geojson)
    
  }
  
}