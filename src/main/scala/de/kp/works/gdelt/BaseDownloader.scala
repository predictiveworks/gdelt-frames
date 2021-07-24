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
import de.kp.works.spark.Session
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.io.{BufferedReader, File, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.util.zip.ZipInputStream
import scala.sys.process._

object DownloadUtil extends Serializable {
 
  val DELIMITER:String = "|"
  val timeout = 5000

  def downloadFile(endpoint:String, fileName:String):Unit = {

    val url = new URL(endpoint)
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

trait BaseDownloader[T] {
  
  protected val session: SparkSession = Session.getSession
  protected val sc: SparkContext = session.sparkContext

  protected var date:String = ""
  protected var partitions:Int = sc.defaultMinPartitions

  protected val verbose = true
  
  def setDate(value:String):T = {
    date = value
    this.asInstanceOf[T]
  }
  
  def setPartitions(value:Int):T = {
    partitions = value
    this.asInstanceOf[T]
  }
  
  def getSession: SparkSession = session
  
  def filesToDF(inpath:String, outfile:String):DataFrame = {
    /*
     * STEP #1: Read file(s) as RDD[String]
     */
    val rdd = sc.binaryFiles(inpath, partitions)
      .flatMap{ case(name:String, content:PortableDataStream) =>

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
      }
      .map(line => {   
        /*
         * __MOD__ GDELT knowledge graph files leverage ';' and '#'
         * to specify list entries
         */
        val text = line
          .replace("\r", "")
          .replace("\n", "")
          .replace("\t", DownloadUtil.DELIMITER)

        Row(text)
      })
    /*
     * STEP #2: Transform file(s) into a dataframe 
     */
    val schema = StructType(Array(StructField("line", StringType, nullable = true)))
    val dataframe = session.createDataFrame(rdd, schema)
    /*
     * STEP #3: Write dataframe as *.csv file and reload
     * with CSV format
     */
    dataframe.write.mode(SaveMode.Overwrite).csv(outfile)      
    session.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", DownloadUtil.DELIMITER)
      .option("quote", " ")
      .csv(outfile)
    
  }

} 