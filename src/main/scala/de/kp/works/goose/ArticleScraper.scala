package de.kp.works.goose
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
import org.apache.commons.lang.StringUtils

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.gravity.goose.{Configuration, Goose}

import de.kp.works.gdelt.functions._
import de.kp.works.goose.Constants._

trait ScraperParams extends Params with DefaultParamsWritable {
  /*
   * INPUT
   */
  val sourceUrlCol = new Param[String](this, "sourceUrlCol", 
      "The name of the column that contains the source Url(s).")

  def setSourceUrlCol(value: String): this.type = set(sourceUrlCol, value)
  setDefault(sourceUrlCol -> "SourceUrl")
  /*
   * IMAGE OUTPUT
   */
  val imageUrlColumn = new Param[String](this, "imageUrlColumn", 
      "The name of the column that contains the HTML header image Url.")
      
  val imageBase64Column = new Param[String](this, "imageBase64Column", 
      "The name of the column that contains the HTML header image in Base64 encoding.")

  def setImageUrlCol(value: String): this.type = set(imageUrlColumn, value)
  setDefault(imageUrlColumn -> "")

  def setImageBase64Col(value: String): this.type = set(imageBase64Column, value)
  setDefault(imageBase64Column -> "")
  
  /*
   * IMAGE MAGICK SUPPORT 
   */
  val magickConvertPath = new Param[String](this, "magickConvertPath", 
      "The path to the ImageMagick Convert executable.")
      
  val magickIdentifyPath = new Param[String](this, "magickIdentifyPath", 
      "The path to the ImageMagick Identify executable.")
  
  def setMagickConvertPath(value: String): this.type = set(magickConvertPath, value)
  setDefault(magickConvertPath -> "/usr/local/bin/convert")

  def setMagickIdentifyPath(value: String): this.type = set(magickIdentifyPath, value)
  setDefault(magickIdentifyPath -> "/usr/local/bin/identify")
      
  /*
   * CONNECTION PARAMS
   */
  val userAgent = new Param[String](this, "userAgent", 
      "The user agent that is sent with web requests to extract Url content.")
      
  val socketTimeout = new Param[Int](this, "socketTimeout", 
      "The socket timeout in milliseconds.")
      
  val connectionTimeout = new Param[Int](this, "connectionTimeout",
      "The connection timeout in milliseconds.")
  
  def setUserAgent(value: String): this.type = set(userAgent, value)
  setDefault(userAgent -> "Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8")

  def setSocketTimeout(value: Int): this.type = set(socketTimeout, value)
  setDefault(socketTimeout -> 10000)

  def setConnectionTimeout(value: Int): this.type = set(connectionTimeout, value)
  setDefault(connectionTimeout -> 10000)

  /*
   * ANNOTATIONS
   */    
  val annotationCols = new Param[List[String]](this, "annotationCols", 
      "The names of the content columns used to annotate each GDELT event.")
  
  def setAnnotationCols(value: List[String]): this.type = set(annotationCols, value)
  setDefault(annotationCols -> List.empty[String])
      
}
/**
 * An Apache Spark Transformer to fetch the web content 
 * that is referenced by a GDELT event url.
 */
class ArticleScraper(override val uid: String) extends Transformer with ScraperParams{
  
  def this() = this(Identifiable.randomUID("articleScraper"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    /*
     * STEP #1: Restrict dataframe to the url column
     * and ensure that each URL is scraped once
     */
    require(
      dataset.schema.exists(s => s.name == $(sourceUrlCol) && s.dataType == StringType), 
      "Field [" + $(sourceUrlCol) + "] is not valid.")

    if ($(annotationCols).isEmpty) return dataset.toDF
    /*
     * Check whether the provided annotator columns
     * match pre-defined annotators  
     */
    $(annotationCols).foreach(colname => {
      if (ANNOTATORS.contains(colname) == false) {
        throw new Exception(s"Annotation `${colname}` is not supported.")
      }
    })
    
    val samples = dataset
      .select($(sourceUrlCol))
      .dropDuplicates($(sourceUrlCol))

    /*
     * STEP #2: Transform sampleset into RDD and perform web
     * scraping for each iteration; the results are exposed
     * as individual annotation columns 
     */
    val rdd = samples.rdd.mapPartitions(rows => {
      /*
       * Configure Goose for each partition; this also
       * covers the descision whether to fetch images. 
       */
      val conf = new Configuration()
      
      if (StringUtils.isNotEmpty($(imageUrlColumn)) || StringUtils.isNotEmpty($(imageBase64Column))) {
        
        conf.setEnableImageFetching(true)
        
        conf.setImagemagickConvertPath($(magickConvertPath))
        conf.setImagemagickIdentifyPath($(magickIdentifyPath))
        
      } else {
        conf.setEnableImageFetching(false)
        
      }
      /* 
       * Mimic browser request and set connection
       * parameters
       */
      conf.setBrowserUserAgent($(userAgent))

      conf.setSocketTimeout($(socketTimeout))
      conf.setConnectionTimeout($(connectionTimeout))
      
      val goose = new Goose(conf)
      /*
       * Scrape each URL individually and retrieve
       * the result as an iterator of [Content]
       */
      val articles = scrapeContent(rows.map(_.getAs[String]($(sourceUrlCol))), goose)
      /*
       * Convert articles to [Rows]s
       */
      articles.map(article => {
        
        val annotations: Seq[Any] = ${annotationCols}.map{colname =>
          colname match {
            case ANNOTATOR_TITLE        => article.title.getOrElse("")
            case ANNOTATOR_DESCRIPTION  => article.description.getOrElse("")
            case ANNOTATOR_CONTENT      => article.content.getOrElse("")
            case ANNOTATOR_KEYWORDS     => article.keywords
            case ANNOTATOR_PUBLISH_DATE => article.publishDate.orNull
            case ANNOTATOR_IMAGE_URL    => article.imageURL.getOrElse("")
            case ANNOTATOR_IMAGE_BASE64 => article.imageBase64.getOrElse("")
          }
          
        }
        .toSeq        
        
        Row.fromSeq(Seq(article.url) ++ annotations)
        
      })
    })      
    
    val session = dataset.sparkSession
    val resultset = session.createDataFrame(rdd, transformSchema(samples.schema))
    
    resultset.join(dataset, List($(sourceUrlCol)))
    
  }

  override def transformSchema(schema:StructType): StructType = {

    StructType(
      schema.seq ++ ${annotationCols}.map{colname =>
        colname match {
          case ANNOTATOR_TITLE        => StructField(colname, StringType, nullable = false)
          case ANNOTATOR_DESCRIPTION  => StructField(colname, StringType, nullable = false)
          case ANNOTATOR_CONTENT      => StructField(colname, StringType, nullable = false)
          case ANNOTATOR_KEYWORDS     => StructField(colname, ArrayType.apply(StringType), nullable = false)
          case ANNOTATOR_PUBLISH_DATE => StructField(colname, DateType, nullable = true)
          case ANNOTATOR_IMAGE_URL    => StructField(colname, StringType, nullable = true)
          case ANNOTATOR_IMAGE_BASE64 => StructField(colname, StringType, nullable = true)
        }
      }
    )
    
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

}