package com.sevenhub

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.PostStop

object StoppingActors {

  object sensitiveActor {
    def apply(): Behavior[String] = Behaviors
      .receive[String] { (context, message) =>
        context.log.info(s"Received message: $message")
        if (message == "You're ugly")
          Behaviors.stopped
        else
          Behaviors.same
      }
      .receiveSignal { // partialFunction
        case (context, PostStop) =>
          // cleanup resources
          context.log.info("I'm stopping")
          Behaviors.same // not used anymore in case of stopping, but will be used for other signals
      }
  }
  def main(args: Array[String]): Unit = {

    val userGuardian = Behaviors.setup[Unit] { context =>
      val sensitive = context.spawn(sensitiveActor(), "SensitiveActor")

      sensitive ! "Hi"
      sensitive ! "How're you"
      sensitive ! "You're ugly"
      sensitive ! "Sorry about that"

      Behaviors.empty

    }

    val system = ActorSystem(userGuardian, "UserGuardianSystem")

    Thread.sleep(1000)
    system.terminate()

  }
}
