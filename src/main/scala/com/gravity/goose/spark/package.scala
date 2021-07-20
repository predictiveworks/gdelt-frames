package com.gravity.goose
/**
 * Licensed to Gravity.com under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Gravity.com licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.sql.Date

import org.apache.commons.lang.StringUtils

import scala.util.Try

package object spark {

  val ANNOTATOR_TITLE = "title"
  val ANNOTATOR_CONTENT = "content"
  val ANNOTATOR_DESCRIPTION = "description"
  val ANNOTATOR_KEYWORDS = "keywords"
  val ANNOTATOR_PUBLISH_DATE = "publishDate"

  // List of supported annotators
  val ANNOTATORS = Array(
    ANNOTATOR_TITLE,
    ANNOTATOR_CONTENT,
    ANNOTATOR_DESCRIPTION,
    ANNOTATOR_KEYWORDS,
    ANNOTATOR_PUBLISH_DATE
  )

  def scrapeArticles(it: Iterator[String], goose: Goose): Iterator[GooseArticle] = {
    
    it.map(url => {
    
      Try {
        val article = goose.extractContent(url)
        GooseArticle(
          url         = url,
          title       = if (StringUtils.isNotEmpty(article.title)) Some(article.title) else None,
          content     = if (StringUtils.isNotEmpty(article.cleanedArticleText)) Some(article.cleanedArticleText.replaceAll("\\n+", "\n")) else None,
          description = if(StringUtils.isNotEmpty(article.metaDescription)) Some(article.metaDescription) else None,
          keywords    = if(StringUtils.isNotEmpty(article.metaKeywords)) article.metaKeywords.split(",").map(_.trim.toUpperCase) else Array.empty[String],
          publishDate = if(article.publishDate != null) Some(new Date(article.publishDate.getTime)) else None,
          image       = None
        )
      } getOrElse GooseArticle(url)
    
    })
  }

  case class GooseArticle(
     url: String,
     title: Option[String] = None,
     content: Option[String] = None,
     description: Option[String] = None,
     keywords: Array[String] = Array.empty[String],
     publishDate: Option[Date] = None,
     image: Option[String] = None)
}
