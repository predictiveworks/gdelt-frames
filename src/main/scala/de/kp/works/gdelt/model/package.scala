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

package object model {
  
  case class Location(
      /*
       * This field specifies the geographic resolution of the match type and 
       * holds one of the following values: This can be used to filter events by 
       * geographic specificity, for example, extracting only those events with 
       * a landmark-level geographic resolution for mapping.
       */
      geoType:String = "*",
      /*
       * This is the full human-readable name of the matched location. In the case 
       * of a country it is simply the country name. For US and World states it is 
       * in the format of "State, Country Name", while for all other matches it is 
       * in the format of "City/Landmark, State, Country".
       * 
       * This can be used to label locations when placing events on a map.
       */
      geoName:String = "*",
      /*
       * This is the 2-character FIPS10-4 country code for the location.
       */
      countryCode:String = "*",
      /*
       * This is the 2-character FIPS10-4 country code followed by the 2-character 
       * FIPS10-4 administrative division 1 (ADM1) code for the administrative division 
       * housing the landmark.
       * 
       * In the case of the United States, this is the 2-character shortform of the 
       * state’s name (such as "TX" for Texas).
       */
      adm1Code:String = "*",
      /*
       * For international locations this is the numeric Global Administrative Unit 
       * Layers (GAUL) administrative division 2 (ADM2) code assigned to each global 
       * location, while for US locations this is the two-character shortform of the 
       * state’s name (such as "TX" for Texas) followed by the 3-digit numeric county 
       * code (following the INCITS 31:200x standard used in GNIS).
       */
      adm2Code:String = "*",
      /*
       * The geo coordinates (lat, lon)
       */
      geoPoint:Seq[Double] = Seq.empty[Double],
      /*
       * This is the GNS or GNIS FeatureID for this location. More information on these 
       * values can be found in Leetaru (2012).
       */
      featureId:String = "*")
}