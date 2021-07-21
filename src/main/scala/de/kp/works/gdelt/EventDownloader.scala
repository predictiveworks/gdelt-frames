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
import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.spark.SparkFiles
import org.apache.spark.input.PortableDataStream

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import de.kp.works.gdelt.functions._
import de.kp.works.spark.Session
/**
 * The base downloader of GDELT event files
 */
class EventDownloader {
  
  private val base = "http://data.gdeltproject.org/events"
  private val verbose = true
  
  private val session = Session.getSession
  private val sc = session.sparkContext
  
  private var date:String = ""
  private var path:String = ""
  
  private var partitions:Int = sc.defaultMinPartitions
  /*
   * http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
   */
  private val columns = Array(
    ("_c0",  "EventId",      "Int"),
    ("_c1",  "Day",          "Int"),
    ("_c2",  "MonthYear",    "Int"),
    ("_c3",  "Year",         "Int"),
    ("_c4",  "FractionDate", "Float"),

    /* ACTOR 1: Initiator */
    ("_c5",  "Actor1_Code",           "String"),
    ("_c6",  "Actor1_Name",           "String"),
    ("_c7",  "Actor1_CountryCode",    "String"),
    ("_c8",  "Actor1_KnownGroupCode", "String"),
    ("_c9",  "Actor1_EthnicCode",     "String"),
    ("_c10", "Actor1_Religion1Code",  "String"),
    ("_c11", "Actor1_Religion2Code",  "String"),
    ("_c12", "Actor1_Type1Code",      "String"),
    ("_c13", "Actor1_Type2Code",      "String"),
    ("_c14", "Actor1_Type3Code",      "String"),
    
    /* ACTOR 2 Recipient or Victim*/
    ("_c15", "Actor2_Code",           "String"),
    ("_c16", "Actor2_Name",           "String"),
    ("_c17", "Actor2_CountryCode",    "String"),
    ("_c18", "Actor2_KnownGroupCode", "String"),
    ("_c19", "Actor2_EthnicCode",     "String"),
    ("_c20", "Actor2_Religion1Code",  "String"),
    ("_c21", "Actor2_Religion2Code",  "String"),
    ("_c22", "Actor2_Type1Code",      "String"),
    ("_c23", "Actor2_Type2Code",      "String"),
    ("_c24", "Actor2_Type3Code",      "String"),

    ("_c25", "IsRootEvent",    "Int"),
    ("_c26", "EventCode",      "String"),
    ("_c27", "EventBaseCode",  "String"),
    ("_c28", "EventRootCode",  "String"),
    ("_c29", "QuadClass",      "Int"),    
    ("_c30", "GoldsteinScale", "Float"),
    ("_c31", "NumMentions",    "Int"),
    ("_c32", "NumSources",     "Int"),
    ("_c33", "NumArticles",    "Int"),
    ("_c34", "AvgTone",        "Float"),
    
    /* ACTOR 1 GEO */
    ("_c35", "Actor1_Geo_Type",        "Int"),
    ("_c36", "Actor1_Geo_Fullname",    "String"),
    ("_c37", "Actor1_Geo_CountryCode", "String"),
    ("_c38", "Actor1_Geo_ADM1Code",    "String"),
    ("_c39", "Actor1_Geo_Lat",         "Float"),
    ("_c40", "Actor1_Geo_Long",        "Float"),
    ("_c41", "Actor1_Geo_FeatureID",   "String"),
    
    /* ACTOR 2 GEO */
    ("_c42", "Actor2_Geo_Type",        "Int"),
    ("_c43", "Actor2_Geo_Fullname",    "String"),
    ("_c44", "Actor2_Geo_CountryCode", "String"),
    ("_c45", "Actor2_Geo_ADM1Code",    "String"),
    ("_c46", "Actor2_Geo_Lat",         "Float"),
    ("_c47", "Actor2_Geo_Long",        "Float"),
    ("_c48", "Actor2_Geo_FeatureID",   "String"),
    
    /* EVENT GEO */
    ("_c49", "Action_Geo_Type",        "Int"),    
    ("_c50", "Action_Geo_Fullname",    "String"),
    ("_c51", "Action_Geo_CountryCode", "String"),
    ("_c52", "Action_Geo_ADM1Code",    "String"),
    ("_c53", "Action_Geo_Lat",         "Float"),
    ("_c54", "Action_Geo_Long",        "Float"),
    ("_c55", "Action_Geo_FeatureID",   "String"),
    
    ("_c56", "DateAdded", "String"),
    /*
		 *  This field records the URL or citation of the first 
		 *  news report it found this event in. In most cases this 
		 *  is the first report it saw the article in, but due to 
		 *  the timing and flow of news reports through the processing 
		 *  pipeline, this may not always be the very first report, but 
		 *  is at least in the first few reports.    
 		 */
    ("_c57", "SourceUrl", "String")
    
  )

  def setDate(value:String):EventDownloader = {
    date = value
    this
  }

  def setPartitions(value:Int):EventDownloader = {
    partitions = value
    this
  }

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
      
      val file = SparkFiles.get(fname)
      /*
       * STEP #2: Read a certain daily file as RDD[String]
       */
      val rdd = sc.binaryFiles(file, partitions)
        .flatMap{ case(name:String, content:PortableDataStream) => {
          
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry)
            .takeWhile {
                case null => zis.close(); false
                case _ => true
            }
            .flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }          
        }}
        .map(line => {          
          val text = line.replace("\r", "").replace("\t", ";")
          Row(text)
        })
      /*
       * STEP #3: Transform the daily event file into a
       * dataframe and persist in local repository
       */
      val schema = StructType(Array(StructField("line", StringType, true)))
      val dataframe = session.createDataFrame(rdd, schema)

      val outfile = s"${path}/${date}.export.csv"
      dataframe.write.mode(SaveMode.Overwrite).csv(outfile)
      
      var input = session.read
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .option("delimiter", ";")
        .option("quote", " ")
        .csv(outfile)
      /*
       * The result contains 58 columns
       */
      columns.foreach{ case(oldName:String, newName:String, _skip:String) => 
        input = input.withColumnRenamed(oldName, newName)
      }
      
      input = input.withColumn("EventId", event_id_udf(col("EventId")))
      
      /*
       * STEP #4: Semantic enrichment
       */
      val enricher = new Enricher()
      enricher.transform(input)

    } catch {
      case t:Throwable => t.printStackTrace();session.emptyDataFrame
    }
   
  }
}
