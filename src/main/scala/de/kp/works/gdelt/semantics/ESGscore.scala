package de.kp.works.gdelt.semantics
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

import de.kp.works.core.FSHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
/**
 * This class is responsible for creating a data-driven
 * ESG (Environmental, Social & Governance) score.
 *
 * This score is not subjective and based on assumptions
 * derived from companies’ official disclosures.
 *
 * It is just the opposite, truly data-driven and derived
 * from how companies’ reputations are perceived in the media.
 *
 * ESG defines an 1:n relation between a GDELT organisation and
 * GDELT themes.
 */
class ESGscore extends ESGbase[ESGscore] {

  /**
   * STAGE #1: Restrict the graph dataset to those articles that
   * refer to the ESG dimensions. This method filters the `Themes`
   * column, explodes `Organisations` and finally selects and
   * renames:
   *
   * publishDate, url, themes, organisation, tone
   *
   * The dataframe is saved to the repository as parquet file.
   *
   * NOTE: We recommend to enrich the `organisation` with a list
   * of alternative organisation names to make sure that GDELT
   * and external naming of a certain organisation with match
   */
  def extract(graph:DataFrame, table:String):Unit = {

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
    samples.write.mode(SaveMode.Overwrite).parquet(s"$repository/esg/$table.extracted.parquet")

  }

  /**
   * STAGE #2: Enrich the organisation name with a list of
   * alternate names to support join operations with other
   * datasets.
   *
   * At the end of this stage, each organisation has assigned
   * a list of themes (or ESG dimensions). What is finally
   * needed is a time series of scores for each organisation
   * and dimension.
   */
  def enrich(table:String, mapping:Map[String, Seq[String]]):Unit = {

    def enrich_organisation_udf(mapping:Map[String, Seq[String]]) =
      udf((organisation:String) => {
        if (mapping.contains(organisation)) {
          Seq(organisation) ++ mapping(organisation)
        }
        else
          Seq(organisation)
      })

    val enrich_organisation = enrich_organisation_udf(mapping)(col("organisation"))

    val samples = session.read.parquet(s"$repository/esg/$table.extracted.parquet")

    samples.withColumn("organisation", explode(enrich_organisation))
    samples.write.mode(SaveMode.Overwrite).parquet(s"$repository/esg/$table.enriched.parquet")

  }

  /**
   * STAGE #3
   */
  def aggregate(table:String):Unit = {
    /*
     * Explode `themes` and aggregate by `publishDate`, `organisation`
     * and `theme`
     */
    val groupCols = List("publishDate", "organisation", "theme").map(col)

    val samples = session.read.parquet(s"$repository/esg/$table.enriched.parquet")
      .withColumn("theme", explode(col("themes")))
      .drop("themes")
      .groupBy(groupCols: _*)
      .agg(count("*").as("total"), sum(col("tone")).as("tone"))
      .withColumnRenamed("publishDate", "date")
    /*
     * The result is a dataset that specifies an organization with
     * the date, theme (ESG dimension), summed tone and the total
     * number of mentions. This prepares for computing our own
     * ESG score
     */
    samples.write.mode(SaveMode.Overwrite).parquet(s"$repository/esg/$table.aggregated.parquet")

  }

  /**
   * STAGE #4
   */
  def score(table:String):Unit = {
    /*
     * A more or less straightforward approach for scoring
     * is used, which looks at the difference between an
     * organisation's tone and its industry average.
     *
     * How much more "positive" or "negative" a company is
     * perceived across all financial services news articles.
     *
     * By looking at the average of that difference over day, and
     * normalizing across industries, a score for each ESG dimension
     * is computed.
     */
    val samples = session.read.parquet(s"$repository/esg/$table.enriched.parquet")

    /*
     * STEP #1: Compute the industry average (sum) with respect
     * to each ESG dimension
     */
    val baseline = samples.groupBy("date", "theme")
      .agg(
        sum(col("tone")).as("ref_tone"),
        sum(col("total")).as("ref_total"))

    /*
     * STEP #2: Join the samples dataset with the baseline
     */
    val result = samples
      .join(baseline, usingColumns = Seq("date", "theme"))
      /*
       * Next we build the difference between the industry
       * tone and an organisation's tone for each ESG dimension
       */
      .withColumn("diff_tone", col("tone") - col("ref_tone"))
      /*
       * We also build a confidence value between the industry
       * mentions and an organisation's mention in new articles
       */
      .withColumn("confidence", col("total") / col("ref_total"))

    result.write.mode(SaveMode.Overwrite).parquet(s"$repository/esg/$table.scored.parquet")

  }
}
