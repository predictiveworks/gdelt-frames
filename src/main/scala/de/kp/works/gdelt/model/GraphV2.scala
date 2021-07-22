package de.kp.works.gdelt.model
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
 */object GraphV2 {
  
  val columns = Array(
    ("_c0",  "RecordId",              "String"),
    ("_c1",  "PublisDate",            "Long"),
    ("_c2",  "SourceCollectionId",    "String"),
    ("_c3",  "SourceCommonName",      "String"),
    ("_c4",  "DocumentIdentifier",    "String"),
    ("_c5",  "Counts",                "List"),
    ("_c6",  "EnhancedCounts",        "List"),
    ("_c7",  "Themes",                "List"),
    ("_c8",  "EnhancedThemes",        "List"),
    ("_c9",  "Locations",             "List"),
    ("_c10", "EnhancedLocations",     "List"),
    ("_c11", "Persons",               "List"),
    ("_c12", "EnhancedPersons",       "List"),
    ("_c13", "Organisations",         "List"),
    ("_c14", "EnhancedOrganisations", "List"),
    ("_c15", "Tone",                  "List"),
    ("_c16", "EnhancedDates",         "List"),
    ("_c17", "Gcams",                 "List"),
    ("_c18", "SharingImage",          "String"),
    ("_c19", "RelatedImages",         "List"),
    ("_c20", "SocialImageEmbeds",     "List"),
    ("_c21", "SocialVideoEmbeds",     "List"),
    ("_c22", "Quotations",            "List"),
    ("_c23", "AllNames",              "List"),
    ("_c24", "Amounts",               "List"),
    ("_c25", "TranslationInfo",       "String"),
    ("_c26", "ExtraXML",              "String")
      
  )
  
}
