package de.kp.works.gdelt.semantics

import de.kp.works.core.FSHelper
import org.apache.spark.sql.{DataFrame, SaveMode, functions}
import org.apache.spark.sql.functions._

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
import de.kp.works.spark.Session

class ESGScore {
  /**
   * This class is responsible for creating a data-driven
   * ESG (Environmental, Social & Governance) score.
   *
   * This score is not subjective and based on assumptions
   * derived from companies’ official disclosures.
   *
   * It is just the opposite, truly data-driven and derived
   * from how companies’ reputations are perceived in the media.
   */

  private val session = Session.getSession
  private val sc = session.sparkContext

  private var repository:String = ""

  def setRepository(value:String):ESGScore = {
    repository = value
    this
  }
  /*
   * STAGE: Restrict the graph dataset to those articles that
   * refer to the ESG dimensions. This method filters the `Themes`
   * column, explodes `Organisations` and finally selects and
   * renames:
   *
   * publishDate, url, themes, organisation, tone
   *
   * The dataframe is saved to the repository as parquet file
   */
  def extract(graph:DataFrame, file:String):Unit = {

    if (repository.isEmpty)
      throw new Exception(s"No path provided for the data repository")
    /*
     * GDELT themes provides a large amount of different semantic
     * dimensions. In this class, we focus on sentiment analysis
     * ([Tone] column) for financial news related articles.
     *
     * This approach is based on the following assumptions:
     *
     * (1) Financial news articles are well captured by the GDELT
     * taxonomy starting with ECON_*.
     *
     * (2) Environmental articles are captured as ENV_* .
     *
     * (3) Social articles are captured by UNGP_* (UN guiding principles
     * on human rights).
     */

    val filter_themes_udf = udf((themes:Seq[String]) => {
      val remain = themes.flatMap(theme => {
        theme.split("_").head match {
          case "ENV"  => Some("E")
          case "ECON" => Some("G")
          case "UNGP" => Some("S")
          case _ => None:Option[String]
        }
      })
      /*
       * Any article, regardless of environmental or social must be ECON_
       */
      if (remain.contains("G"))
        remain.distinct
      else
        Seq.empty[String]

    })
    val selCols = List("PublishDate", "DocumentIdentifier", "Themes", "Organisations", "Tone").map(col)
    val samples = graph
      /*
       * Restrict available new articles to those that
       * match the described filter criteria
       */
      .withColumn("Themes", filter_themes_udf(col("Themes")))
      .filter(size(col("Themes") > 0))
      .select(selCols: _*)
      /*
       * Explode the `Organizations` column to enable
       * joins with other datasets
       */
      .withColumn("organisation", explode(col("Organizations")))
      .drop("Organisations")
      /*
       * Finally the column names are renamed
       */
      .withColumnRenamed("PublishDate", "publishDate")
      .withColumnRenamed("DocumentIdentifier", "url")
      .withColumnRenamed("Themes", "themes")
      .withColumnRenamed("Tone", "tone")

    /*
     * Persist samples as parquet file
     */
    FSHelper.checkIfNotExistsCreate(sc, s"$repository/esg")
    samples.write.mode(SaveMode.Overwrite).parquet(s"$repository/esg/$file")

  }
}
