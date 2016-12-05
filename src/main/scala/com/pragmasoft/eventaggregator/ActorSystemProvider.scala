package com.pragmasoft.eventaggregator

import akka.actor.ActorSystem

trait ActorSystemProvider {
  implicit def actorSystem: ActorSystem
}
