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

import de.kp.works.core.FSHelper
import de.kp.works.gdelt.enrich.{EventEnricher, GraphEnricher}
import de.kp.works.gdelt.functions._
import de.kp.works.gdelt.model.{EventV2, GraphV2, MentionV2}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class FileDownloader extends BaseDownloader[FileDownloader] {
  
  private val uri = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
  private var repository:String = ""
  
  def setRepository(value:String):FileDownloader = {
    repository = value
    this
  }
  /**
   * This method loads all `events` into a single
   * dataframe and persists the result as parquet
   * file.
   * 
   * Note: The events downloaded from the masterfiles
   * contain 61 columns and does not match with events
   * downloaded with the EventDownloader.
   */
  def prepareEvents:DataFrame = {
    
    val inpath = s"$repository/event/*.export.CSV.zip"
    val outfile = s"$repository/events.csv"

    var input = filesToDF(inpath, outfile)
    /*
     * The result contains 61 columns
     */
    EventV2.columns.foreach{ case(oldName:String, newName:String, _skip:String) => 
      input = input.withColumnRenamed(oldName, newName)
    }
    /*
    * Remove all columns from the dataset that
    * are unresolved
    */
    val dropCols = input.columns.filter(colname => colname.startsWith("_c"))

    input = input
      .withColumn("EventId", event_id_udf(col("EventId")))
      .drop(dropCols: _*)
    /*
     * Semantic enrichment
     */
    val enricher = new EventEnricher().setVersion("V2")
    enricher.transform(input)

  }
  /**
   * This method loads all `graphs` into a single
   * dataframe and persists the result as parquet
   * file.
   */
  def prepareGraphs:DataFrame = {
    
    val inpath = s"$repository/graph/*.gkg.csv.zip"
    val outfile = s"$repository/graphs.csv"

    var input = filesToDF(inpath, outfile)
    /*
     * The result contains 27 columns
     */
    GraphV2.columns.foreach{ case(oldName:String, newName:String, _skip:String) => 
      input = input.withColumnRenamed(oldName, newName)
    }
    /*
    * Remove all columns from the dataset that
    * are unresolved
    */
    val dropCols = input.columns.filter(colname => colname.startsWith("_c"))
    input = input.drop(dropCols: _*)
    /*
     * Semantic enrichment
     */
    val enricher = new GraphEnricher()
    enricher.transform(input)

  }  
  /**
   * This method loads all `mentions` into a single
   * dataframe and persists the result as parquet
   * file.
   */
  def prepareMentions():DataFrame = {
    
    val inpath = s"$repository/mention/*.mentions.CSV.zip"
    val outfile = s"$repository/mentions.csv"

    var input = filesToDF(inpath, outfile)
    /*
     * The result contains 14 columns
     */
    MentionV2.columns.foreach{ case(oldName:String, newName:String, _skip:String) => 
      input = input.withColumnRenamed(oldName, newName)
    }
    /*
     * Remove all columns from the dataset that
     * are unresolved
     */
    val dropCols = input.columns.filter(colname => colname.startsWith("_c"))

    input = input
      .withColumn("EventId", event_id_udf(col("EventId")))
      .drop(dropCols: _*)

    input

  }
  
  /**
   * This method defines the second stage of a GDELT ingestion
   * pipe and leverages the `masterfiles` to download all referenced
   * files. As a result, 3 different  folder are filled, each for
   * events, graphs and mentions.
   */

  def downloadFiles(from:Long = -1L, to:Long = -1L):Unit = {
    
    val startts = System.currentTimeMillis
    val masterfiles = session.read.parquet(s"$repository/masterfiles.parquet")
    
    val ts0 = System.currentTimeMillis
    if (verbose) println(s"Masterfiles loaded in ${ts0 - startts} ms.")

    val timestamp_udf = udf((url:String) => {
      /*
       * Split the url by `/` and leverage last token,
       * which describes the file name.
       */
      val fname = url.split("\\/").last
      /*
       * The split by `.` and use the first token,
       * which is the datetime representation
       */
      val datetime = fname.split("\\.").head
      /*
       * The datetime is formatted as follows: [YYYY][MM][DD][HH][MM][SS]
       */
      val year = datetime.substring(0, 4).toInt
      val month = datetime.substring(4, 6).toInt

      val day = datetime.substring(6, 8).toInt
      val hour = datetime.substring(8, 10).toInt

      val minute = datetime.substring(10, 11).toInt
      val second = datetime.substring(12, 14).toInt

      val calendar = java.util.Calendar.getInstance

      calendar.set(java.util.Calendar.YEAR, year)
      calendar.set(java.util.Calendar.MONTH, month)

      calendar.set(java.util.Calendar.DAY_OF_MONTH, day)
      calendar.set(java.util.Calendar.HOUR_OF_DAY, hour)

      calendar.set(java.util.Calendar.MINUTE, minute)
      calendar.set(java.util.Calendar.SECOND, second)

      calendar.getTimeInMillis

    })
    /*
     * STEP #1: Enhance materfiles dataset with timestamp
     * and enable filtering with respect to a time interval
     */
    val annotated = masterfiles.withColumn("timestamp", timestamp_udf(col("url")))

    val filtered =
      if (from == -1L && to == -1L)
        annotated

      else if (from == -1L)
        annotated.filter(col("timestamp") <= to)

      else if (to == -1L)
        annotated.filter(col("timestamp") >= from)

      else
        annotated.filter(col("timestamp") >= from && col("timestamp") <= to)

    /*
     * STEP #2: Download filtered event files
     * from masterfiles
     */
    download(filtered, "event")

    val ts1 = System.currentTimeMillis
    if (verbose) println(s"Event files downloaded in ${ts1 - ts0} ms.")
    /*
     * STEP #3: Download filtered graph files
     * from masterfiles
     */
    download(filtered, "graph")

    val ts2 = System.currentTimeMillis
    if (verbose) println(s"Graph files downloaded in ${ts2 - ts1} ms.")
    /*
     * STEP #2: Download filtered mention files
     * from masterfiles
     */
    download(filtered, "mention")

    val ts3 = System.currentTimeMillis
    if (verbose) println(s"Mention files downloaded in ${ts3 - ts2} ms.")

  }
  
  private def download(masterfiles:DataFrame, category:String):Unit = {
    /*
     * Check whether folder within repository exists
     * and if not create
     */
    FSHelper.checkIfNotExistsCreate(sc, s"$repository/$category")

    val files = masterfiles.filter(col("category") === category)
    /*
     * __MOD__
     *
     * In-method reference required; otherwise the
     * task is not serializable
     */
    val path = repository
    files.select("url").repartition(100).foreach(row => {
      
       val endpoint = row.getAs[String](0)
       val fileName = s"$path/$category/${row.getAs[String](0).split("/").last}"
       
       DownloadUtil.downloadFile(endpoint, fileName)
       
    })
  }
  /**
   * This method marks the first stage of a GDELT ingestion
   * pipe and downloads the `masterfiles`, transforms its
   * content into a DataFrame, enriches each file with the
   * respective `category` and persists the result as a 
   * parquet file.
   */
  def prepareFiles(year:String):Unit = {
    /*
     * STEP #1: Download the masterfile list from GDELT
     * and restrict its content to the provided year,
     * and finally save as parquet file.
     */
    downloadMasterFiles()
    /*
     * STEP #2: Read master file list with Apache Spark
     */
    val fileName = s"$repository/masterfiles.csv"
    val masterfiles = session.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", " ")
      .option("quote", " ")
      .csv(fileName)
      /*
       * As the master file does not contain header
       * information, the respective columns are 
       * added
       */
      .withColumnRenamed("_c0","size")
      .withColumnRenamed("_c1","hash")
      .withColumnRenamed("_c2","url")

    /*
     * STEP #3: The files are restricted to a certain year
     * and a semantic category is added to ease the download
     * of files that refer to events, mentions or GDELT's 
     * knowledge graph
     */
    val category_udf = udf((url:String) => {
      
      if (url.endsWith(".CSV.zip") || url.endsWith(".csv.zip")) {

        if (url.contains(".gkg")) 
          "graph"

        else if (url.contains(".mentions"))
          "mention"
        
        else 
          "event"
            
      } else "unknown"
      
    })  

    masterfiles
      .where(col("url").like(s"%/$year%"))
      .select("size", "url")
      .withColumn("category", category_udf(col("url")))
      .filter(not(col("category") === "unknown"))
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$repository/masterfiles.parquet")
    
  }
  
  private def downloadMasterFiles():Unit = {
    
    if (repository.isEmpty)
      throw new Exception("No repository provided.")
    
    val fileName = s"$repository/masterfiles.csv"
    DownloadUtil.downloadFile(uri, fileName)
    
  }
}