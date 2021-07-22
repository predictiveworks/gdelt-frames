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
import org.apache.spark.sql._

class TvApi extends BaseApi[TvApi] {
  /**
   * https://blog.gdeltproject.org/gdelt-2-0-television-api-debuts/
   */
  private val base = "URL: https://api.gdeltproject.org/api/v2/tv/tv"
  
  private def request(query:String, params:Map[String,String]):DataFrame = {
    
    val encoded = encodeText(query)
    val urlPart = paramsToUrl(params)
    
    val endpoint = s"${base}?query=${encoded}${urlPart}"
    csvToDataFrame(endpoint)

  }
  
}