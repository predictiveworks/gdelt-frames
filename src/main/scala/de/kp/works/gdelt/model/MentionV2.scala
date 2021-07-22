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
 */

object MentionV2 {
  
  val columns = Array(
    ("_c0",  "EventId",        "Int"),
    ("_c1",  "EventTime",      "Int"),
    ("_c2",  "MentionTime",    "Int"),
    ("_c3",  "MentionType",    "Int"),
    ("_c4",  "SourceName",     "String"),
    ("_c5",  "SourceIdent",    "String"),
    ("_c6",  "SentenceId",     "Int"),
    ("_c7",  "Actor1Offset",   "Int"),
    ("_c8",  "Actor2Offset",   "Int"),
    ("_c9",  "ActionOffset",   "Int"),
    ("_c10", "InRawText",      "Int"),
    ("_c11", "Confidence",     "Int"),
    ("_c12", "MentionDocLen",  "Int"),
    ("_c13", "MentionDocTone", "Float")    
  )
  
}