package de.kp.works.gdelt.transform
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

import de.kp.works.gdelt.model.Location
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, struct, udf}

class Transactioner extends BaseTransform[Transactioner] {

  private val columns = List("Locations", "Organisations", "Persons", "Themes")
  /**
   * This method transforms a set of columns of the GDELT
   * graph dataset (*.gkg.*) into a transaction column to
   * enable frequent item and association rule mining.
   */
  def transform(graph:DataFrame):DataFrame = {
    /*
     * STEP #1: Preprocessing
     *
     * Preprocessing of the individual column content
     */
    def locations_udf = udf((locations:Seq[Location]) => {

      if (locations == null)
        Seq.empty[String]

      else {
        locations.map(location => {
          /*
           * The location term used for the transaction
           * build concatenates name, resolution and index
           * to enable appropriate post processing.
           */
          s"${location.geoName}|${location.resolution}|${location.geoIndex}"
        })
      }
    })

    def preprocess_udf(colname:String) =
      udf((values:Seq[String]) => {

        if (values == null)
          Seq.empty[String]

        else {
          /*
           * Compute the postfix that identifies
           * the respective column entry
           */
          val postfix = colname match {
            case "Locations"     => "_LOC"
            case "Organisations" => "_ORG"
            case "Persons"       => "_PER"
            case "Themes"        => "_THE"
            case _ => ""
          }

          values
            .filter(_.nonEmpty)
            .distinct
            .map(value =>
              value.replace(" ", "") + postfix)
        }
      })

    var samples = graph.withColumn("Locations", locations_udf(col("Locations")))

    columns.foreach(column =>
      samples = samples
        .withColumn(column, preprocess_udf(column)(col(column)))
    )
    /*
     * STEP #2: Transaction
     *
     * Merge the columns into single `transaction` column
     */
    val transaction_udf = udf((row:Row) => {

      val locations = row.getAs[Seq[String]]("Locations")
      val organisations = row.getAs[Seq[String]]("Organisations")

      val persons = row.getAs[Seq[String]]("Persons")
      val themes = row.getAs[Seq[String]]("Themes")

      val transaction = locations ++ organisations ++ persons ++ themes
      /*
       * Apache Spark's FPGrowth algorithms expects a distinct
       * set of items within a transaction
       */
      transaction.distinct

    })

    val colstruct = struct(columns.map(col): _*)
    samples = samples
      .withColumn("transaction", colstruct)

    /*
     * STEP #3: Transaction identifier
     *
     * Now there is an appropriate `transaction` defined;
     * what is missing, is a unique transaction identifier
     */
    val identCols = List("RecordId", "PublishDate", "DocumentIdentifier")
    val identStruct = struct(identCols.map(col): _*)

    val ident_udf = udf((row:Row) => {

      val recordId    = row.getAs[String]("RecordId")
      val publishDate = row.getAs[String]("PublishDate")
      val documentId  = row.getAs[String]("DocumentIdentifier")

      s"$recordId|$publishDate|$documentId"
    })
    /*
     * The result is a 2 column dataset that is prepared
     * to be used with Apache Spark's FPGrowth
     */
    samples
      .withColumn("taid", identStruct)
      .select("taid", "transaction")

  }
}
