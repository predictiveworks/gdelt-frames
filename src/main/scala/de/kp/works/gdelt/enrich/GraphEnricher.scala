package de.kp.works.gdelt.enrich
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
import org.apache.spark.sql.functions._
import de.kp.works.gdelt.functions._
import de.kp.works.gdelt.GDELTModel

class GraphEnricher {

  private val countryCodes = GDELTModel.countryCodes

  def transform(graph:DataFrame):DataFrame = {
    /*
     * Locations & enhanced locations
     */
    var enriched = graph
      .withColumn("Locations", locations_udf(countryCodes)(col("Locations")))
      .withColumn("EnhancedLocations", enhanced_locations_udf(countryCodes)(col("EnhancedLocations")))
    /*
     * Organizations & enhanced organizations
     */
    enriched = enriched
      .withColumn("Organisations", organisations_udf(col("Organisations")))
      .withColumn("EnhancedOrganisations", enhanced_organisations_udf(col("EnhancedOrganisations")))
      
    /*
     * Persons & enhanced persons
     */
    enriched = enriched
      .withColumn("Persons", persons_udf(col("Persons")))
      .withColumn("EnhancedPersons", enhanced_persons_udf(col("EnhancedPersons")))
      
    /*
     * Themes & enhanced themes
     */
    enriched = enriched
      .withColumn("Themes", themes_udf(col("Themes")))
      .withColumn("EnhancedThemes", enhanced_themes_udf(col("EnhancedThemes")))

    enriched
  }
}