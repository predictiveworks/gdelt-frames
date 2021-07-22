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

import org.apache.spark.SparkFiles

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


import de.kp.works.gdelt.functions._
import de.kp.works.gdelt.model.EventV1

/**
 * The base downloader of GDELT event files
 */
class EventDownloader extends BaseDownloader[EventDownloader] {
  
  private val base = "http://data.gdeltproject.org/events"
  private var path:String = ""

  def setRepository(value:String):EventDownloader = {
    path = value
    this
  }

  def download:DataFrame = {
    /*
     * The date is expected as YYYYMMDD
     */
    val fname = s"${date}.export.CSV.zip"
    val uri = s"${base}/${fname}"
    
    try {
      /*
       * STEP #1: Download event file to distributed
       * file system; if the file is available already,
       * an exception is thrown
       */
      try {
        sc.addFile(uri)} 
      
      catch {
        case t:Throwable => if (verbose) println(s"The file `${fname}` already exists")
      }
      /*
       * STEP #2: Transform event file(s) into a dataframe
       */
      val inpath  = SparkFiles.get(fname)
      val outfile = s"${path}/${date}.export.csv"

      var input = filesToDF(inpath, outfile)
      /*
       * The result contains 58 columns
       */
      EventV1.columns.foreach{ case(oldName:String, newName:String, _skip:String) => 
        input = input.withColumnRenamed(oldName, newName)
      }
      
      input = input.withColumn("EventId", event_id_udf(col("EventId")))
      
      /*
       * STEP #3: Semantic enrichment
       */
      val enricher = new Enricher()
      enricher.transform(input)

    } catch {
      case t:Throwable => t.printStackTrace();session.emptyDataFrame
    }
   
  }
}
