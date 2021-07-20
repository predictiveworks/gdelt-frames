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
import scala.collection.mutable

class ContextApi extends BaseApi[ContextApi] {
  /**
   * https://blog.gdeltproject.org/announcing-the-gdelt-context-2-0-api/
   */
  private val base = "https://api.gdeltproject.org/api/v2/context/context"
  
  /*
   * - MAXRECORDS
   * 
   * In article list mode, the API only returns up 75 results by default,
   * but this can be increased up to 250 results if desired by using this 
   * URL parameter.
   */
  def article(query:String, mode:String="artlist", maxRecords:Int = 75, timespan:Int=3, timerange:String="month"):DataFrame = {
    /*
     * ArtList. There is only one mode at this time and you must specify "artlist"
     */
    if (mode != "artlist")
      throw new Exception("The mode provided does not refer to article queries.")
    
    val params = mutable.HashMap.empty[String,String]
    
    params += "mode" -> mode
    params += "timespan" -> getTimespan(timespan, timerange)
    
    request(query, params.toMap)

  }
  
  private def request(query:String, params:Map[String,String]):DataFrame = {
    
    val encoded = encodeText(query)
    val urlPart = paramsToUrl(params)
    
    val endpoint = s"${base}?query=${encoded}${urlPart}"
    csvToDataFrame(endpoint)

  }
  
}