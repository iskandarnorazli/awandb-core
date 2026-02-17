package org.awandb.server.auth

import org.apache.arrow.flight.auth2.{BasicCallHeaderAuthenticator, CallHeaderAuthenticator}
import org.apache.arrow.flight.CallStatus

class AwanAuthHandler extends BasicCallHeaderAuthenticator(
  new BasicCallHeaderAuthenticator.CredentialValidator {
    override def validate(username: String, password: String): CallHeaderAuthenticator.AuthResult = {
      
      if (username == "admin" && password == "admin") {
        // [FIX] Instantiate the Java interface directly
        new CallHeaderAuthenticator.AuthResult {
          override def getPeerIdentity: String = "admin"
        }
      } else {
        throw CallStatus.UNAUTHENTICATED.withDescription("Invalid AwanDB Credentials").toRuntimeException
      }
    }
  }
)