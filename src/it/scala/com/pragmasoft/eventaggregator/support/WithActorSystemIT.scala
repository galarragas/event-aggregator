package com.pragmasoft.eventaggregator.support

import akka.actor.ActorSystem

trait WithActorSystemIT {
  def withActorSystem(block: ActorSystem => Unit) = {
    val actorSystem = ActorSystem("TestFlow")

    try block(actorSystem) finally actorSystem.terminate()
  }
}
