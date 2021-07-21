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

import org.apache.spark._

import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import de.kp.works.spark.Session

trait BaseDownloader[T] {
  
  protected val session = Session.getSession
  protected val sc = session.sparkContext

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
  
  def getSession = session
  
  def filesToDF(path:String):DataFrame = {
    /*
     * STEP #1: Read file(s) as RDD[String]
     */
    val rdd = sc.binaryFiles(path, partitions)
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
     * STEP #2: Transform file(s) into a dataframe 
     */
    val schema = StructType(Array(StructField("line", StringType, true)))
    session.createDataFrame(rdd, schema)
    
  }
} 