package de.kp.works.gdelt.semantics

import org.apache.spark.sql.functions.{col, collect_list, explode, udf}

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
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

class ESGgraph extends ESGbase[ESGgraph] {

  private var edgeThreshold:Int = 200

  def setEdgeThreshold(value:Int):ESGgraph = {
    edgeThreshold = value
    this
  }

  /**
   * This method builds an ESG network and starts with
   * the `enriched` dataset from [ESGscore]
   */
  def transform(table:String):GraphFrame = {

    val samples = session.read.parquet(s"$repository/esg/$table.enriched.parquet")
    /*
     * STEP #1: Extract the nodes or vertices
     */
    val nodes = samples
      .select(col("organisation").as("id")).distinct()
    /*
     * STEP #2: Build edges
     *
     * GDELT has nasty habit to categorize `united states` or `european`
     * as organisations. We can also remove nodes we know are common, such
     * as reuters.
     */
    val blacklist = Set(
      "united states",
      "european union",
      "reuters",
      "twitter",
      "facebook",
      "thomson reuters",
      "associated press",
      "new york times",
      "bloomberg")

    def build_pairs_udf(blacklist: Set[String]) = udf((organisations: Seq[String]) => {
      /*
       * Restrict to organisations that are not mentioned in the blacklist
       */
      val remain = organisations.filter(o => !blacklist.contains(o))
      remain.flatMap(o1 => {
        remain.map(o2 => {
          (o1, o2)
        })
      }).filter { case (o1, o2) => o1 != o2 }
    })

    /*
     * The network is build as co-occurrence graph of organisations
     * mentioned in the same article.
     */
    val edges = samples
      .groupBy(col("url"))
      .agg(collect_list(col("organisation")).as("organisations"))
      .drop("organisation")
      .withColumn("pairs", build_pairs_udf(blacklist)(col("organisations")))
      .withColumn("pair", explode(col("pairs")))
      .withColumn("src", col("pair._1"))
      .withColumn("dst", col("pair._2"))
      /*
       * The frequency of the observed (src,dst) pairs is interpreted
       * as strength of the edge or relation
       */
      .groupBy("src", "dst")
      .agg(sum(lit(1)).as("relationship"))
      .filter(col("relationship") > edgeThreshold)

    GraphFrame(nodes, edges)

  }
}
