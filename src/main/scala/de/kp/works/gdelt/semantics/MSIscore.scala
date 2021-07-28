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
/**
 * This class is responsible
 */
class MSIscore {
  /**
   * A curated list of GDELT themes that are used for
   * certain index dimensions: the list is retrieved
   * from BBVA Research any may be customized to meet
   * own requirements.
   */
  val FISCAL_POLICY_INDEX = List(
    "AUSTERITY",
    "PRIVATIZATION",
    "ECON_DEBT",
    "ECON_SUBSIDIES",
    "ECON_TAXATION",
    "WB_450_DEBT",
    "WB_1116_FISCAL_CONSOLIDATION",
    "WB_445_FISCAL_POLICY",
    "WB_2773_FISCAL_POLICY_AND_JOBS",
    "WB_449_FISCAL_RISKS",
    "WB_769_INVESTMENT_POLICY",
    "WB_716_MANAGING_PUBLIC_FINANCES",
    "WB_713_PUBLIC_FINANCE",
    "WB_1121_TAXATION",
    "WB_1122_TAX_EXPENDITURES",
    "WB_720_TAX_AND_REVENUE_POLICY_AND_ADMINISTRATION",
    "WB_382_TAX_CREDITS_AND_DIRECT_SUBSIDIES",
    "WB_856_WAGES"
  )

  val MONETARY_POLICY_INDEX = List(
    "FUELPRICES",
    "ECON_COST_OF_LIVING",
    "ECON_CURRENCY_EXCHANGE_RATE",
    "ECON_CURRENCY_RESERVES",
    "ECON_INTEREST_RATES",
    "WB_1235_CENTRAL_BANKS",
    "WB_442_INFLATION",
    "WB_444_MONETARY_POLICY",
    "WB_1124_EXCHANGE_RATE_POLICY"
  )

  val POLITICAL_INDEX = List(
    "GOV_REFORM",
    "DEMOCRACY",
    "ELECTION",
    "GENERAL_GOVERNMENT",
    "POLITICAL_PARTY",
    "POLITICAL_TURMOIL"
  )

  val GLOBAL_POLICY_INDEX = List(
    "ECON_CENTRALBANK",
    "ECON_INTEREST_RATES",
    "EPU_POLICY",
    "EPU_POLICY_CENTRAL_BANK",
    "EPU_POLICY_GOVERNMENT",
    "EPU_POLICY_LAW",
    "EPU_POLICY_LEGISLATION",
    "EPU_POLICY_MONETARY_POLICY",
    "EPU_POLICY_REGULATION",
    "EPU_POLICY_REGULATORY",
    "EPU_POLICY_SPENDING",
    "EPU_CATS_MONETARY_POLICY",
    "EPU_POLICY_MONETARY_POLICY",
    "WB_1125_INTEREST_RATE_POLICY",
    "WB_444_MONETARY_POLICY",
    "WB_1235_CENTRAL_BANKS",
    "EPU_POLICY_INTEREST_RATE",
    "EPU_POLICY_INTEREST_RATES"
  )

}
