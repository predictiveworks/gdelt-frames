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

object Constants {
  /*
   * The names of the annotation columns that can be added
   * after scraping the content of a GDELT event url.
   */
  val ANNOTATOR_TITLE        = "annotated_title"
  val ANNOTATOR_CONTENT      = "annotated_content"
  val ANNOTATOR_DESCRIPTION  = "annotated_description"
  val ANNOTATOR_KEYWORDS     = "annotated_keywords"
  val ANNOTATOR_PUBLISH_DATE = "annotated_publishDate"
  val ANNOTATOR_IMAGE_URL    = "annotated_imageURL"
  val ANNOTATOR_IMAGE_BASE64 = "annotated_imageBase64"
  /*
   * The names of the annotation columns made available as
   * a List to support validation tasks.
   */
  val ANNOTATORS = List(
    ANNOTATOR_TITLE,
    ANNOTATOR_CONTENT,
    ANNOTATOR_DESCRIPTION,
    ANNOTATOR_KEYWORDS,
    ANNOTATOR_PUBLISH_DATE,
    ANNOTATOR_IMAGE_URL,
    ANNOTATOR_IMAGE_BASE64)

}