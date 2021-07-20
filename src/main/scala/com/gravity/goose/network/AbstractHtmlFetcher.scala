package com.gravity.goose.network
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

import com.gravity.goose.Configuration
import org.apache.http.client.HttpClient

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 10/13/12
 * Time: 1:02 AM
 *
 * The workhorse of goose. Override the {@see com.gravity.goose.network.HtmlFetcher} within your configuration for complete control.
 */
trait AbstractHtmlFetcher {
  /**
   * Access the `url` over the internet and retrieve the HTML from it
   * @param config overrides and tweaks
   * @param url the address to access and retrieve content from
   * @return `Some` `String` of the response from the specified `url` or `None` if failed to retrieve HTML.
   */
  def getHtml(config: Configuration, url: String): Option[String]

  /**
   * A shared accessor for making image calls
   * @return a fully configured and initialized instance for shared use
   */
  def getHttpClient: HttpClient
}
