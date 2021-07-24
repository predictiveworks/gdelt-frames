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

case class Theme(term:String, count:Long)

class Taxonomy {

  def filterTax(themes:Seq[Theme], filter:String):Seq[String] = {

    val prefix = s"TAX_$filter"
    themes
      .filter(theme => theme.term.startsWith(prefix))
      .map(_.term)
      .sorted
  }

  /**
   * The result is a list of 21 topics:
   *
   * C01_CHILDREN_AND_EDUCATION
   * C02_NEEDSPROVIDE_FOOD
   * C03_WELLBEING_HEALTH
   * C04_LOGISTICS_TRANSPORT
   * C05_NEED_OF_SHELTERS
   * C06_WATER_SANITATION
   * C07_SAFETY
   * C08_TELECOM
   * CRISISLEXREC
   * O01_WEATHER
   * O02_RESPONSEAGENCIESATCRISIS
   * T01_CAUTION_ADVICE
   * T02_INJURED
   * T03_DEAD
   * T04_INFRASTRUCTURE
   * T05_MONEY
   * T06_SUPPLIES
   * T07_SERVICESNEEDEDOFFERED
   * T08_MISSINGFOUNDTRAPPEDPEOPLE
   * T09_DISPLACEDRELOCATEDEVACUATED
   * T11_UPDATESSYMPATHY
   */
  def loadCrisisLexGroups(themes:Seq[Theme]):Seq[String] = {

    themes
      .filter(theme => theme.term.startsWith("CRISISLEX_"))
      .map(theme => {
        val term = theme.term.replace("CRISISLEX_", "")

        val tokens = term.split("_")
        term
      })
      .distinct
      .sorted

  }
  /**
   * The result is a list of 64 topics:
   *
   * BANKRUPTCY
   * BITCOIN
   * BOYCOTT
   * BUBBLE
   * BUDGET_DEFICIT
   * CENTRALBANK
   * COST_OF_LIVING
   * COUNTERFEITMONEY
   * CURRENCY_EXCHANGE_RATE
   * CURRENCY_RESERVES
   * CUTOUTLOOK
   * DEBT
   * DEFLATION
   * DEREGULATION
   * DEVELOPMENTORGS
   * DIESELPRICE
   * EARNINGSREPORT
   * ELECTRICALDEMAND
   * ELECTRICALGENERATION
   * ELECTRICALGRID
   * ELECTRICALLOADSHEDDING
   * ELECTRICALPRICE
   * EMERGINGECON
   * ENTREPRENEURSHIP
   * FOREIGNBANKS
   * FOREIGNINVEST
   * FREETRADE
   * GASOLINEPRICE
   * GOLDPRICE
   * HEATINGOIL
   * HEATINGOILPRICE
   * HOUSING_PRICES
   * IDENTITYTHEFT
   * INFLATION
   * INFORMAL_ECONOMY
   * INTEREST_RATES
   * IPO
   * MIDDLECLASS
   * MONEYLAUNDERING
   * MONOPOLY
   * MOU
   * NATGASPRICE
   * NATIONALIZE
   * NEWPOWERPLANT
   * OILPRICE
   * PAY_CUTS
   * PRICECONTROL
   * PRICEGOUGE
   * PRICEMANIPULATION
   * PROPANE
   * PROPANEPRICE
   * QUITRATE
   * REMITTANCE
   * SOVEREIGN_DEBT
   * STOCKMARKET
   * SUBSIDIES
   * SUSPICIOUSACTIVITYREPORT
   * TAXATION
   * TRADE_DISPUTE
   * TRANSPORT_COST
   * UNDEREMPLOYMENT
   * UNIONS
   * WORKINGCLASS
   * WORLDCURRENCIES
   */
  def loadECONGroups(themes:Seq[Theme]):Seq[String] = {

    themes
      .filter(theme => theme.term.startsWith("ECON_"))
      .map(theme => {
        val term = theme.term.replace("ECON_", "")

        val tokens = term.split("_")
        term
      })
      .distinct
      .sorted

  }

  /**
   * The result is a list of 58 topics:
   *
   * CATS_DEBT_CEILING_GOV_SHUTDOWN
   * CATS_ENTITLEMENT_PROGRAMS
   * CATS_FINANCIAL_REGULATION
   * CATS_FISCAL_POLICY
   * CATS_HEALTHCARE
   * CATS_MIGRATION_FEAR_FEAR
   * CATS_MIGRATION_FEAR_MIGRATION
   * CATS_MONETARY_POLICY
   * CATS_NATIONAL_SECURITY
   * CATS_REGULATION
   * CATS_SOVEREIGN_DEBT_CURRENCY_CRISES
   * CATS_TAXES
   * CATS_TRADE_POLICY
   * ECONOMY
   * ECONOMY_HISTORIC
   * MIGRATION_FEAR_FEAR
   * MIGRATION_FEAR_MIGRATION
   * POLICY
   * POLICY_AUTHORITIES
   * POLICY_BANK_OF_JAPAN
   * POLICY_BANK_OF_KOREA
   * POLICY_BLUE_HOUSE
   * POLICY_BUDGET
   * POLICY_BUDGET_OUTFLOW
   * POLICY_BUDGET_OUTFLOWS
   * POLICY_CENTRAL_BANK
   * POLICY_CONGRESS
   * POLICY_CONGRESSIONAL
   * POLICY_CREDIT_CRUNCH
   * POLICY_DEFICIT
   * POLICY_DUMA
   * POLICY_FEDERAL_RESERVE
   * POLICY_FISCAL_POLICY
   * POLICY_GOVERNMENT
   * POLICY_INTEREST_RATE
   * POLICY_INTEREST_RATES
   * POLICY_LAW
   * POLICY_LAWMAKERS
   * POLICY_LEGISLATION
   * POLICY_MINISTRY_OF_FINANCE
   * POLICY_MONETARY_POLICY
   * POLICY_NATIONAL_SPENDING
   * POLICY_PEOPLES_BANK_OF_CHINA
   * POLICY_PEOPLE_BANK_OF_CHINA
   * POLICY_POLICY
   * POLICY_POLICYMAKERS
   * POLICY_POLITICAL
   * POLICY_PUBLIC_INVESTMENT
   * POLICY_PUBLIC_UNDERTAKING
   * POLICY_PUBLIC_UNDERTAKINGS
   * POLICY_REFORM
   * POLICY_REGULATION
   * POLICY_REGULATORY
   * POLICY_SPENDING
   * POLICY_TAX
   * POLICY_WHITE_HOUSE
   * POLICY_WORLD_TRADE_ORGANIZATION
   * UNCERTAINTY
   */
  def loadEPUGroups(themes:Seq[Theme]):Seq[String] = {

    themes
      .filter(theme => theme.term.startsWith("EPU_"))
      .map(theme => {
        val term = theme.term.replace("EPU_", "")

        val tokens = term.split("_")
        term
      })
      .distinct
      .sorted

  }

  /**
   * This is the initial load method to retrieve
   * the actual themes used by GDELT. The result
   * is a list of 26 topics:
   *
   * AGRICULHARMINSECTS
   * AIDGROUPS
   * CARTELS
   * CHRONICDISEASE
   * DISEASE
   * ECON
   *
   * This semantic category contains 2 categories:
   *
   * - FREETRADEAGREEMENTS
   * - PRICE
   *
   * ETHNICITY
   * FNCACT
   * FOODSTAPLES (staple foot, foot staples)
   * MILITARY
   * PLANTDISEASE
   * POLITICAL
   * RELIGION
   * SPECIAL
   * SPECIALDEATH
   * TERROR
   * WEAPONS
   *
   * The remaining topics refer to nature, except
   * LANGUAGES
   *
   * WORLDARACHNIDS
   * WORLDBIRDS
   * WORLDCRUSTACEANS
   * WORLDFISH
   * WORLDINSECTS
   * WORLDLANGUAGES
   * WORLDMAMMALS
   * WORLDMYRIAPODA
   * WORLDREPTILES
   *
   * Note: Concepts or topics, prefixed with TAX_ cover more
   * than 94% of all semantic concepts (themes)
   */
  def loadTaxGroups(themes:Seq[Theme]):Seq[String] = {

    themes
      .filter(theme => theme.term.startsWith("TAX_"))
      .map(theme => {
        val term = theme.term.replace("TAX_", "")

        val tokens = term.split("_")
        tokens(0)
      })
      .distinct
      .sorted

  }

  /**
   * This is a list of actually 1857 different categories
   * that cover less than 3% of the total themes.
   */
  def loadWBGroups(themes:Seq[Theme]):Seq[String] = {

    themes
      .filter(theme => theme.term.startsWith("WB_"))
      .map(theme => {
        val term = theme.term.replace("WB_", "")
        val tokens = term.split("_")

        term
      })
      .distinct
      .sorted

  }

  def loadThemes():Seq[Theme] = {

    val is = this.getClass.getResourceAsStream("themes-202107.txt")
    scala.io.Source.fromInputStream(is, "UTF-8").getLines()
      /* Ignore header */
      .toSeq.drop(1)
      .map(line => {

        val tokens = line.split(",")
        val term = tokens(0).trim
        val count = tokens(1).trim.toLong

        Theme(term, count)

      })
  }
}
