/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.server.middleware

import org.apache.arrow.flight.{CallHeaders, CallInfo, CallStatus, FlightServerMiddleware, RequestContext}

/**
 * Intercepts incoming Arrow Flight RPC calls and extracts the requested format.
 */
class FormatMiddleware(val format: String) extends FlightServerMiddleware {
  override def onBeforeSendingHeaders(outgoingHeaders: CallHeaders): Unit = {}
  override def onCallCompleted(status: CallStatus): Unit = {}
  override def onCallErrored(err: Throwable): Unit = {}
  
  def getFormat(): String = format
}

/**
 * Factory attached to the FlightServer to instantiate the middleware per-request.
 */
class FormatMiddlewareFactory extends FlightServerMiddleware.Factory[FormatMiddleware] {
  override def onCallStarted(info: CallInfo, incomingHeaders: CallHeaders, context: RequestContext): FormatMiddleware = {
    var format = "string" // Default to standard OLTP string parsing
    
    if (incomingHeaders != null) {
      // .get() returns a plain String in the Arrow Java API
      val reqFormat = incomingHeaders.get("x-awan-format")
      if (reqFormat != null && reqFormat.nonEmpty) {
        format = reqFormat.toLowerCase
      }
    }
    new FormatMiddleware(format)
  }
}

object FormatMiddleware {
  val Key = FlightServerMiddleware.Key.of[FormatMiddleware]("awan-format-key")
}