package de.kp.works.gdelt

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