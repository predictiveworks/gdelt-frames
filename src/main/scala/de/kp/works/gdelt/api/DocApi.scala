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
import scala.collection.mutable

/**
 * The [[DocApi]] is restricted to the `csv `format
 */
class DocApi extends BaseApi[DocApi] {
  /**
   * https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
   */
  private val base = "https://api.gdeltproject.org/api/v2/doc/doc"
  /*
   * `mode` specifies the specific output you would like from the API, 
   * ranging from timelines to word clouds to article lists.
   */
  private val MODES = List(
    /* 
     * This is the most basic output mode and generates a simple list of 
     * news articles that matched the query. In HTML mode articles are 
     * displayed in a table with its social sharing image (if available) 
     * to its left, the article title, its source country, language and 
     * publication date all shown. 
     * 
     * RSS output format is only available in this mode.
     */
    "artlist",
    /*
     * This displays the same information as the "ArtList" mode, but does 
     * so using a "high design" visual layout suitable for creating magazine-style 
     * collages of matching coverage. Only articles containing a social sharing 
     * image are included.
     */
    "artgallery",
    /*
     * This displays all matching images that have been processed by the GDELT Visual 
     * Global Knowledge Graph (VGKG), which runs each image through Google's Cloud 
     * Vision API deep learning image cataloging. 
     * 
     * If your query does not contain any image-related search terms, this mode will 
     * return a list of all VGKG-processed images that were contained in the body of 
     * matching articles, while if your search included image terms, only matching 
     * images will be shown. 
     * 
     * Thus, this mode is most relevant when used with the various image-related query 
     * terms. Each image is provided with a link to the article containing it. 
     * 
     * Note that the document extraction system used by GDELT may on occasion make 
     * mistakes and associate an image with a news article in which it appeared only 
     * as an inset or unrelated footer, though this is usually rare. 
     * 
     * This mode is most useful for understanding the visual portrayal of your search.
     */
    "imagecollage",
    /*
     * This yields identical output as the ImageCollage option, but adds four additional 
     * pieces of information to each image: 
     * 
     * 1) the number of times (up to 200) it has been seen before on the open web (via a 
     * reverse Google Images search), 
     * 
     * 2) a list of up to 6 of those web pages elsewhere on the web where the image was 
     * found in the past, 
     * 
     * 3) the date the photograph was captured via in the image's internal metadata (EXIF/etc), 
     * and 
     * 
     * 4) a warning if the image's embedded date metadata suggests the photograph was taken 
     * more than 72 hours prior to it appearing in the given article. 
     * 
     * Using this information you can rapidly triage which of the returned images are heavily-used 
     * images and which are novel images that have never been found anywhere on the web before by 
     * Google's crawlers.
     * 
     * (You can also use the "imagewebcount" query term above to restrict your search to just images 
     * which have appeared a certain number of times.) Only a relatively small percent of news images 
     * contain an embedded capture datestamp that documents the date and time the image was taken or 
     * created and it is not always accurate, but where available this can offer a powerful indicator
     * that a given image may be older than it appears and for applications that rely on filtering 
     * for only novel images (such as crisis mapping image cataloging), this can be used as a signal 
     * to perform further verification on an image.
     */
    "imagecollageinfo",
    /*
     * This displays most of the same information as the "ImageCollageInfo" mode (though it does not 
     * include the embedded date warning), but does so using a "high design" visual layout suitable 
     * for creating magazine-style collages of matching coverage.
     */
    "imagegallery",
    /*
     * Instead of returning VGKG-processed images, this mode returns a list of the social sharing images 
     * found in the matching news articles. Social sharing images are those specified by an article to be 
     * shown as its image when shared via social media sites like Facebook and Twitter. 
     * 
     * Not all articles include social sharing images and the images may sometimes only be the logo of 
     * the news outlet or not representative of the article contents, but in general they offer a reasonable 
     * visual summary of the core focus of the article and especially how it will appear when shared across
     * social media platforms.
     */
    "imagecollageshare",
    /*
     * This is the most basic timeline mode and returns the volume of news coverage that matched your query 
     * by day/hour/15 minutes over the search period. Since the total number of news articles published 
     * globally varies so much through the course of a day and through the weekend and holiday periods, 
     * the API does not return a raw count of matched articles, but instead divides the number of matching
     *  articles by the total number of all articles monitored by GDELT in each time step. 
     *  
     *  Thus, the timeline reports volume as a percentage of all global coverage monitored by GDELT. 
     *  For time spans of less than 72 hours, the timeline uses a time step of 15 minutes to provide 
     *  maximum temporal resolution, while for time spans from 72 hours to one week it uses an hourly 
     *  resolution and for time spans of greater than a week it uses a daily resolution. 
     *  
     *  In HTML mode the timeline is displayed as an interactive browser-based visualization.
     */
    "timelinevol",
    /* 
     * This is identical to the standard TimelineVol mode, but instead of reporting results as a percent 
     * of all online coverage monitored by GDELT, it returns the actual number of distinct articles that 
     * matched your query. In CSV and JSON output modes, an additional "norm" field is returned that 
     * records the total number of all articles GDELT monitored during that time interval – NOTE that 
     * this norm field is NOT smoothed when smoothing is enabled.
     */
    "timelinevolraw", 
    /*
     * This is identical to the main TimelineVol mode, but for each time step it displays the top 10 most 
     * relevant articles that were published during that time interval. Thus, if you see a sudden spike in 
     * coverage of your topic, you can instantly see what was driving that coverage. In HTML mode a popup 
     * is displayed over the timeline as you mouse over it and you can click on any of the articles to view 
     * them, while in JSON and CSV mode the article list is output as part of the file.
     */
    "timelinevolinfo",
    /*
     * Similar to the main TimelineVol mode, but instead of coverage volume it displays the average 
     * "tone" of all matching coverage, from extremely negative to extremely positive.
     */
    "timelinetone",
    /*
     * Similar to the TimelineVol mode, but instead of showing total coverage volume, it breaks coverage 
     * volume down by language so you can see which languages are focusing the most on a topic. 
     * 
     * Note that the GDELT APIs currently only search the 65 machine translated languages supported by GDELT, 
     * so stories trending in unsupported languages will not be displayed in this graph, but will likely be 
     * captured by GDELT as they are cross-covered in other languages. With the launch of GDELT3 later this 
     * summer, the resolution and utility of this graph will increase dramatically.
     */
    "timelinelang",
    /*
     * Similar to the TimelineVol mode, but instead of showing total coverage volume, it breaks coverage 
     * volume down by source country so you can see which countries are focusing the most on a topic. 
     * 
     * Note that GDELT attempts to monitor as much media as possible in each country, but smaller countries 
     * with less developed media systems will necessarily be less represented than larger countries with 
     * massive local press output. With the launch of GDELT3 later this summer, the resolution and utility 
     * of this graph will increase dramatically.
     */
    "timelinesourcecountry",
    /*
     * This is an extremely powerful visualization that creates an emotional histogram showing the tonal 
     * distribution of coverage of your query. All coverage matching your query over the search time period 
     * is tallied up and binned by tone, from -100 (extremely negative) to +100 (extremely positive). 
     * 
     * (Though typically the actual range will be from -20 to 20 or less). Articles in the -1 to +1 bin tend 
     * to be more neutral or factually-focused, while those on either extreme tend to be emotionally-laden 
     * diatribes. 
     * 
     * Typically most sentiment dashboards display a single number representing the average of all coverage 
     * matching the query ala "The average tone of Donald Trump coverage in the last week is -7". Such displays
     * are not very informative since its unclear what precisely "-7" means in terms of tone and whether that 
     * means that most coverage clustered around -7 or whether it means there were a lot of extremely negative 
     * and extremely positive coverage that averaged out to -7, but no actual coverage around that tonal range. 
     * 
     * By displaying tone as a histogram you are able to see the full distributional curve, including whether 
     * most coverage clusters around a particular range, whether it has an exponential or bell curve, etc. 
     * 
     * In HTML mode you can mouse over each bar to see a popup with the top 10 most relevant articles in that 
     * tone range and click on any of the headlines to view them.
     */
    "tonechart",
    /*
     * This is identical to the WordCloudEnglish mode, but instead of the article text words, this mode 
     * takes all of the VGKG-processed images found in the matching articles (or which matched any image 
     * query operators) and constructs a histogram of the top topics assigned by Google's deep learning 
     * neural network algorithms as part of the Google Cloud Vision API.
     */
    "wordcloudimagetags",
    /*
     * This is identical to the WordCloudImageTags mode, but instead of using the tags assigned by Google's 
     * deep learning algorithms, it uses the Google knowledge graph topical taxonomy tags assigned by the 
     * Google Cloud Vision API's Web Annotations engine. 
     * 
     * This engine performs a reverse Google Images search on each image to locate all instances where it has 
     * been seen on the open web, examines the captions of all of those instances of the image and compiles a 
     * list of topical tags that capture the contents of those captions. In this way this field offers a far 
     * more powerful and higher resolution understanding of the primary topics and activities depicted in the 
     * image, including context that is not visible in the image, but relies on the captions assigned by others, 
     * whereas the WordCloudImageTags field displays the output of deep learning algorithms considering the 
     * visual contents of the image.
     */
    "wordcloudimagewebtags"
  )
  /*
   * TIME SOMOOTHING
   * 
   * This option is only available in the various Timeline modes and performs moving window smoothing over the 
   * specified number of time steps, up to a maximum of 30. Due to GDELT's high temporal resolution, timeline 
   * displays can sometimes capture too much of the chaotic noisy information environment that is the global 
   * news landscape, resulting in jagged displays. Use this option to enable moving average smoothing up to 30 days.  
   * 
   * Note that since this is a moving window average, peaks will be shifted to the right, up to several days or 
   * weeks at the heaviest smoothing levels.
   */

  /**
   * UNUSED QUERY PARAMETERS
   * 
   * - TRANS
   * 
   * Only available in ArticleList mode with HTML output, this embeds a machine translation widget in the results 
   * page to seamlessly machine translate all of the article titles into your requested language. As this API is
   * restricted to CSV output, there is no match.
   * 
   * - SORT
   * 
   * Default is a sorting by relevance and for the purpose of this API, this is completely sufficient.
   * 
   * - TIMEZOOM
   * 
   * This parameter is restriced to the Html format and this API is limited to CSV outputs. Therefore,
   * there is match for this parameter.
   */
  
  /*
   * - MAXRECORDS
   * 
   * In article list mode, the API only returns up 75 results by default,
   * but this can be increased up to 250 results if desired by using this 
   * URL parameter.
   */
  def article(query:String, mode:String, maxRecords:Int = 75, timespan:Int=3, timerange:String="month"):DataFrame = {
    
    if (mode.toLowerCase.startsWith("art") == false)
      throw new Exception("The mode provided does not refer to article queries.")
    
    val params = mutable.HashMap.empty[String,String]
    
    params += "mode" -> mode
    params += "timespan" -> getTimespan(timespan, timerange)
    
    request(query, params.toMap)

  }
  
  def image(query:String, mode:String, timespan:Int=3, timerange:String="month"):DataFrame = {
    
    if (mode.toLowerCase.startsWith("image") == false)
      throw new Exception("The mode provided does not refer to image queries.")
    
    val params = mutable.HashMap.empty[String,String]
    
    params += "mode" -> mode
    params += "timespan" -> getTimespan(timespan, timerange)
    
    request(query, params.toMap)
      
  }
  
  def timeline(query:String, mode:String, smoothing:Int = -1, timespan:Int=3, timerange:String="month"):DataFrame = {
    
    if (mode.toLowerCase.startsWith("timeline") == false)
      throw new Exception("The mode provided does not refer to timeline queries.")
    
    if (smoothing > 30)
      throw new Exception("GDELT does not support smoothing beyond 30 time steps.")
    
    val params = mutable.HashMap.empty[String,String]
    
    params += "mode" -> mode
    params += "timespan" -> getTimespan(timespan, timerange)
    
    if (smoothing != -1)
      params += "timelinesmooth" -> smoothing.toString
    
    request(query, params.toMap)
      
  }
  
  def tonecart(query:String, timespan:Int=3, timerange:String="month"):DataFrame = {
    
    val params = mutable.HashMap.empty[String,String]
    
    params += "mode" -> "tonechart"
    params += "timespan" -> getTimespan(timespan, timerange)
    
    request(query, params.toMap)
    
  }
  
  private def request(query:String, params:Map[String,String]):DataFrame = {
    
    val encoded = encodeText(query)
    val urlPart = paramsToUrl(params)
    
    val endpoint = s"${base}?query=${encoded}${urlPart}&format=csv"
    csvToDataFrame(endpoint)

  }
  
}