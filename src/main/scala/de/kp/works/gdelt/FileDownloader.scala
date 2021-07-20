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
import sys.process._

import java.io.File
import java.net.{HttpURLConnection, URL} 

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import de.kp.works.spark.Session

class FileDownloader {
  
  private val uri = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
  
  private val session = Session.getSession

  private val timeout = 5000
  private var repository:String = ""
  
  def setRepository(value:String):FileDownloader = {
    repository = value
    this
  }
  
  def prepareMasterFiles(year:String):Unit = {
    /*
     * STEP #1: Download the masterfile list from GDELT
     * and restrict its content to the provided year,
     * and finally save as parquet file.
     */
    downloadMasterFiles
    /*
     * STEP #2: Read master file list with Apache Spark
     */
    val fileName = s"${repository}/masterfiles.csv"
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
     */
    masterfiles
      .where(col("url").like(s"%/${year}%"))
      .select("size", "url")
      .write.mode(SaveMode.Overwrite).parquet(s"${repository}/masterfiles.parquet")
    
  }
  
  private def downloadMasterFiles:Unit = {
    
    if (repository.isEmpty)
      throw new Exception("No repository provided.")
    
    val fileName = s"${repository}/masterfiles.csv"
    
    val url = new URL(uri)
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    /*
     * Set connection parameters
     */
    conn.setConnectTimeout(timeout)
    conn.setReadTimeout(timeout)
    
    conn.connect()
    
    if (conn.getResponseCode >= 400)
        println("The download of the GDELT master file failed with: " + conn.getResponseMessage)
        
    else
        url #> new File(fileName) !!
    
  }

}