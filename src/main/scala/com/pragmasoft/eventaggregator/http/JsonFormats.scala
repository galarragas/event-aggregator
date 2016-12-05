package com.pragmasoft.eventaggregator.http

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

trait JsonFormats {

  implicit val formats = Serialization.formats(NoTypeHints)

}
