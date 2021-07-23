package de.kp.works.gdelt.bigquery
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
import de.kp.works.spark.Session

class BigQuery {

  private val session = Session.getSession
  private val reader = session.read.format("bigquery")

  private var dataset:String = ""
  /*
   * The Google Cloud Project ID of the table or query.
   */
  private var project:String = ""
  private var credentialsFile:String = ""

  def setCredentialsFile(value:String):BigQuery = {
    credentialsFile = value
    this
  }

   /**
   * This method sets the name of the materialized dataset.
   * It is required for query requests
   */
  def setDataset(value:String):BigQuery = {
    dataset = value
    this
  }

  def setProject(value:String):BigQuery = {
    project = value
    this
  }

  def loadSql(sql:String):DataFrame = {
    reader
      .option("parentProject", project)
      .option("credentialsFile", credentialsFile)
      /*
       * Enables the connector to read from views and not only tables.
       * Please read the relevant section before activating this option.
       */
        .option("viewsEnabled", "true")
      /*
       * The dataset where the materialized view is going to be created.
       */
      .option("materializationDataset", dataset)
      .option("query", sql)
      .load()

   }

  def loadTable(table:String):DataFrame = {
    reader
      .option("parentProject", project)
      .option("credentialsFile", credentialsFile)
      /*
       * The BigQuery table in the format [[project:]dataset.]table.
       */
      .load(table)
  }

}
