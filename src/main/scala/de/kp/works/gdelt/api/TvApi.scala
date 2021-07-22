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
  
  private val MODES = List(
    /*
		 * This displays up to the top 50 most relevant clips matching your search. Each returned 
		 * clip includes the name of the source show and station, the time the clip aired, a thumbnail, 
		 * the actual text of the snippet and a link to view the full one minute clip on the Internet 
		 * Archive's website. This allows you to see what kinds of clips are matching and view the full 
		 * clip to gain context on your search results. 
     */
    "clipgallery",
    /*
     * This determines what percent of your search results were from each specific television show 
     * and displays the top 10 shows.
     */
    "showhart",
    /*
     * This compares how many results your search generates from each of the selected stations over 
     * the selected time period, allowing you to assess the relative attention each is paying to the 
     * topic. Using the DATANORM parameter you can control whether this reports results as raw clip 
     * counts or as normalized percentages of all coverage (the most robust way of comparing stations). 
     */
    "stationchart",
    /*
     * This is a special mode that outlets the complete list of all stations that are available for 
     * searching, along with the start and end date of their monitoring. Some stations may simply no 
     * longer exist (such as Aljazeera America), while others were monitored by the Internet Archive
     * for a brief period of time around a major event like an election. Stations with end dates within 
     * the last 24 hours can are considered active stations being currently monitored.
     * 
     * Note that this mode is only available with JSON/JSONP output.
     */
    "stationdetails",
    /*
     * This tracks how many results your search generates by day/hour over the selected time period, 
     * allowing you to assess the relative attention each is paying to the topic and how that attention 
     * has varied over time. Using the DATANORM parameter you can control whether this reports results 
     * as raw clip counts or as normalized percentages of all coverage (the most robust way of comparing stations). 
     * 
     * By default, the timeline will not display the most recent 24 hours, since those results are still 
     * being generated (it can take up to 2-12 hours for a show to be processed by the Internet Archive 
     * and ready for analysis), but you can include those if needed via the LAST24 option. 
     * 
     * You can also smooth the timeline using the TIMELINESMOOTH option and combine all selected stations 
     * into a single time series using the DATACOMB option. 
     */
    "timelinevol",
    /*
     * Displays an hourly timeline of total airtime seconds matching the query, rapidly pinpointing hourly 
     * trends, where the X axis is days and Y axis is hours from 0 to 23. Each cell is color-coded from 
     * white (0) to dark blue (maximum value). Note that this visualization is very computationally expensive 
     * and thus may take several seconds or longer to return.
     */
    "timelinevolheatmap",
    /*
     * Same as "TimelineVol" but displays as a streamgraph rather than a line-based timeline.
     */
    "timelinevolstream",
    /*
     * This displays the total airtime (in terms of 15 second clips) monitored from each of the stations in 
     * your query. It must be combined with a valid query, since it displays the airtime for the stations 
     * queried in the search. This mode can be used to identify brief monitoring outages or for advanced 
     * normalization, since it reports the total amount of clips monitored overall from each station in 
     * each day/hour.
     */
    "timelinevolnorm",
    /*
     * This is a special mode that returns the most common trending topics, trending keywords/phrases and 
     * top keywords, overall and for the national stations. The results here offer a powerful summary of the 
     * major topics and memes dominating television news coverage at the moment. Results are updated every 
     * 15 minutes. 
     * 
     * Note that this mode is only available with JSON/JSONP output.
     */
    "trendingtopics",
    /*
     * This mode returns the top words that appear most frequently in clips matching your search. It takes 
     * the 200 most relevant clips that match your search and displays a word cloud of up to the top 200 most 
     * frequent words that appeared in those clips (common stop words are automatically removed). 
     * 
     * This is a powerful way of understanding the topics and words dominating the relevant coverage and 
     * suggesting additional contextual search terms to narrow or evolve your search. Note that if there are 
     * too few matching clips for your query, the word cloud may be blank. 
     */
    "wordcloud"
    )
  
  private def request(query:String, params:Map[String,String]):DataFrame = {
    
    val encoded = encodeText(query)
    val urlPart = paramsToUrl(params)
    
    val endpoint = s"${base}?query=${encoded}${urlPart}&format=csv"
    csvToDataFrame(endpoint)

  }
  
}