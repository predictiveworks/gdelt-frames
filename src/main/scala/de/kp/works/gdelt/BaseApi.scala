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
import java.net.URLEncoder

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import de.kp.works.http.HttpConnect
import de.kp.works.spark.Session

trait BaseApi[T] extends HttpConnect {
  
  protected val session = Session.getSession
  protected val sc = session.sparkContext
  
  protected var folder:String = ""    
  protected var partitions:Int = sc.defaultMinPartitions

  protected def setFolder(value:String):T = {
    folder = value
    this.asInstanceOf[T]
  }

  protected def setPartitions(value:Int):T = {
    partitions = value
    this.asInstanceOf[T]
  }
  
  protected def encodeText(text:String):String = {
    URLEncoder.encode(text, "UTF-8")    
  }

  protected def csvToDataFrame(endpoint:String):DataFrame = {
    
    val bytes = get(endpoint)    

    val lines = extractCsvBody(bytes)    
    val rows = lines.map(line => Row(line.replace(",", ";")))
    
    val rdd = sc.parallelize(rows, partitions)
    
    val schema = StructType(Array(StructField("line", StringType, true)))
    val tempframe = session.createDataFrame(rdd, schema)

    val uuid = java.util.UUID.randomUUID.toString
    
    val tempfile = s"${folder}/${uuid}.csv"
    tempframe.write.mode(SaveMode.Overwrite).csv(tempfile)
      
    val dataframe = session.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ";")
      .option("quote", " ")
      .csv(tempfile)
     
    dataframe
    
  }
}