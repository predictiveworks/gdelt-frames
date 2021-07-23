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
  
  case class Actor(
      /*
    		 * The complete raw CAMEO code for Actor (includes geographic, class, ethnic, 
    		 * religious, and type classes). May be blank if the system was unable to identify 
    		 * an Actor.
       */
      actorCode:String = "*",
      /*
       * The actual name of the Actor. In the case of a political leader or organization, 
       * this will be the leader’s formal name (GEORGE W BUSH, UNITED NATIONS), for a 
       * geographic match it will be either the country or capital/major city name 
       * (UNITED STATES / PARIS), and for ethnic, religious, and type matches it will 
       * reflect the root match class (KURD, CATHOLIC, POLICE OFFICER, etc).
       * 
       * May be blank if the system was unable to identify an Actor.
       */
      actorName:String = "*",
      /*
       * The 3-character CAMEO code for the country affiliation of Actor. May be blank if 
       * the system was unable to identify an Actor1 or determine its country affiliation 
       * (such as "UNIDENTIFIED GUNMEN").
       * 
       * Note: This code is different from the Geo or Location country code
       */
      countryCode:String = "*",
      countryName:String = "*",
      /*
       * If Actor is a known IGO/NGO/rebel organization (United Nations, World Bank, 
       * al-Qaeda, etc) with its own CAMEO code, this field will contain that code.
       */
      groupCode:String = "*",
      groupName:String = "*",
      /*
       * If the source document specifies the ethnic affiliation of Actor and that 
       * ethnic group has a CAMEO entry, the CAMEO code is entered here.
       */
      ethnicCode:String = "*",
      ethnicName:String = "*",
      /*
       * If the source document specifies the religious affiliation of Actor and that 
       * religious group has a CAMEO entry, the CAMEO code is entered here.
       */
      religion1Code:String = "*",
      religion1Name:String = "*",
      /*
       * If multiple religious codes are specified for Actor, this contains the secondary 
       * code. Some religion entries automatically use two codes, such as Catholic, which 
       * invokes Christianity as Code1 and Catholicism as Code2.
       */
      religion2Code:String = "*",
      religion2Name:String = "*",
      /*
       * The 3-character CAMEO code of the CAMEO "type" or "role" of Actor, if specified.
       * This can be a specific role such as Police Forces, Government, Military, Political 
       * Opposition, Rebels, etc, a broad role class such as Education, Elites, Media, Refugees, 
       * or organizational classes like Non-Governmental Movement.
       * Special codes such as Moderate and Radical may refer to the operational strategy of a group.
       */
      type1Code:String = "*",
      type1Name:String = "*",
      /*
       * If multiple type/role codes are specified for Actor, this returns the second code.
       */
      type2Code:String = "*",
      type2Name:String = "*",
      /*
       * If multiple type/role codes are specified for Actor1, this returns the third code.
       */
      type3Code:String = "*",
      type3Name:String = "*")
  
  case class Event(
      /*
       * This defines the root-level category the event code falls under. For example, 
       * code "0251" ("Appeal for easing of administrative sanctions") has a root code 
       * of "02" ("Appeal").
       * 
       * This makes it possible to aggregate events at various resolutions of specificity.
       */
      rootCode:String = "*",
      rootName:String = "*",
      /*
       * AMEO event codes are defined in a three-level taxonomy. For events at level three 
       * in the taxonomy, this yields its level two leaf root node. For example, code "0251" 
       * ("Appeal for easing of administrative sanctions") would yield an EventBaseCode of 
       * "025" ("Appeal to yield").
       * 
       * This makes it possible to aggregate events at various resolutions of specificity.
       * For events at levels two or one, this field will be set to EventCode.
       */
      baseCode:String = "*",
      baseName:String = "*",
      /*
       * This is the raw CAMEO action code describing the action that Actor1 performed 
       * upon Actor2.
       */
      eventCode:String = "*",
      eventName:String = "*")
      
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
      countryName:String = "*",
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
       * The H3 resolution used to compute the geospatial index
       */
      resolution:Int = 7,
      /*
       * The H3 index of the (lat, lon) coordinate
       */
      geoIndex:Long = Long.MaxValue,
      /*
       * This is the GNS or GNIS FeatureID for this location. More information on these 
       * values can be found in Leetaru (2012).
       */
      featureId:String = "*")
      
  case class EnhancedLocation(location:Location = Location(), offset:Int = -1)
  
  case class EnhancedOrganization(organization:String = "*", offset:Int = -1)
  
  case class EnhancedPerson(person:String = "*", offset:Int = -1)
  
  case class EnhancedTheme(theme:String = "*", offset:Int = -1)
  
}