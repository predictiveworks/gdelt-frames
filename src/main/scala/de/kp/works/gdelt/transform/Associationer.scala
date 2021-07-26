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

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

/**
 * This transformer is built to be used with the [Transactioner]
 * to support an FPGrowth based approach to determine the nodes
 * and edges of a GDELT network.
 *
 * Reminder: Transactions are built from the columns `Locations`,
 * `Organisations`, `Persons` and `Themes`.
 */
class Associationer extends BaseTransform[Associationer] {

  private var edgeThreshold:Int = 10

  private var minSupport:Double = 0.2
  /*
   * This parameter effects the association rule mining
   * part of this transformer
   */
  private var minConfidence:Double = 0.6

  def setEdgeThreshold(value:Int):Associationer = {
    edgeThreshold = value
    this
  }

  def setMinConfidence(value:Double):Associationer = {
    minConfidence = value
    this
  }

  def setMinSupport(value:Double):Associationer = {
    minSupport = value
    this
  }

  /**
   * This method transforms a transaction dataset into
   * a GraphFrame.
   */
  def transform(transactions:DataFrame):GraphFrame = {

    validateSchema(transactions)
    /*
     * STEP #1: Build FPGrowth model
     */
    val fpgrowth = new FPGrowth()
      .setItemsCol("transactions").setMinSupport(minSupport).setMinConfidence(minConfidence)

    val model = fpgrowth.fit(transactions)
    /*
     * STEP #2: Compute association rules
     *
     * The result is a dataset that contains 4 columns,
     * `antecedent`, `consequent`, `confidence` and `lift`.
     */
    val rules = model.associationRules
    /*
     * In order to derive strong associations, we evaluate
     * the provided `lift` parameter:
     *
     * 1) A value of lift less than 1 shows that having antecedent
     * in the transaction does not increase the chances of occurrence
     * of consequent, in spite of the rule showing a high confidence
     * value.
     *
     * 2) A value of lift greater than 1 vouches for high association
     * between antecedent and {Y} and {X}. More the value of lift, greater
     * are the chances of preference to see consequent in the transaction
     * if antecedent is present already.
     */
      .filter(col("lift") > 1)
    /*
     * STEP #3: Build edges from the association rules
     */
    val edges_udf = udf((antecedent:Seq[String], consequent:Seq[String]) => {

      antecedent.flatMap(a => {
        consequent.map(c => (a,c))
      })
      /*
       * First, we make sure that a != c, which should
       * be asserted by association rule mining already
       */
      .filter{ case (n1, n2) => n1 != n2 }
      /*
       * Second, we make sure that the nodes forming a
       * pair do not refer to the same category.
       */
      .filter{ case (n1, n2) =>

        val postfix1 = n1.split("_").last
        val postfix2 = n2.split("_").last

        postfix1 != postfix2
      }

    })
    /*
     * The result is a 3-column dataset with `src`, `dst`
     * and `relationship`.
     */
    val edges = rules
      .withColumn("pairs", edges_udf(col("antecedent"), col("consequent")))
      .withColumn("pair", explode(col("pairs")))
      .withColumn("src", col("pair._1"))
      .withColumn("dst", col("pair._2"))
      /*
       * Association rule mining computes complex relations
       * between multiple antecedent and consequent nodes.
       *
       * As we extract pairs from these rules, there is a
       * likeliness that a certain pair occurs more than
       * once. This is interpreted as the strength of this
       * relation
       */
      .groupBy("src", "dst")
      .agg(sum(lit(1)).as("relationship"))
      .filter(col("relationship") > edgeThreshold)

    /*
     * STEP #4: Extract nodes
     */
    val node_udf = udf((src:String, dst:String) => Seq(src,dst))
    val nodes = edges
      .withColumn("pairs", node_udf(col("src"), col("dst")))
      .withColumn("id", explode(col("pairs")))
      .select("id").distinct

    GraphFrame(nodes, edges)

  }

  def validateSchema(dataset:DataFrame):Unit = {

    val columns = dataset.columns

    if (!columns.contains("taid"))
      throw new Exception(s"The dataset provided is no transaction dataset.")

    if (!columns.contains("transaction"))
      throw new Exception(s"The dataset provided is no transaction dataset.")

  }


}
