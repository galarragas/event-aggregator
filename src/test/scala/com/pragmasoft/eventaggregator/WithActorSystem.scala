package com.pragmasoft.eventaggregator

import akka.actor.ActorSystem

trait WithActorSystem {
  def withActorSystem(block: ActorSystem => Unit) = {
    val actorSystem = ActorSystem("TestFlow")

    try block(actorSystem) finally actorSystem.terminate()
  }
}
