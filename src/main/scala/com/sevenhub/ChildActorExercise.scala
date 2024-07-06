package com.sevenhub

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.ActorRef

trait MasterProtocol
case class Initiliaze(nWorker: Int) extends MasterProtocol
case class WordCountTask(text: String, replyTo: ActorRef[UserProtocol])
    extends MasterProtocol
case class WordCountReply(id: Int, count: Int) extends MasterProtocol

trait WorkerProtocol
case class WorkerTask(id: Int, work: String) extends WorkerProtocol

trait UserProtocol
case class Reply(count: Int) extends UserProtocol

object WordCountMaster {
  def apply(): Behavior[MasterProtocol] = Idle()

  def Idle(): Behavior[MasterProtocol] = Behaviors.receive {
    (context, message) =>
      message match {
        case Initiliaze(nWorker) =>
          var workerMap: Map[String, ActorRef[WorkerProtocol]] = Map()
          (0 to nWorker - 1).foreach(x =>
            val workerName = s"Worker$x"
            val workeRef =
              context.spawn(WordCountWorker(context.self), workerName)
            workerMap = workerMap + (workerName -> workeRef)
          )
          Ready(nWorker, 0, workerMap, Map())
      }
  }

  def Ready(
      nWorkers: Int,
      nextWorkerId: Int,
      workers: Map[String, ActorRef[WorkerProtocol]],
      users: Map[Int, ActorRef[UserProtocol]]
  ): Behavior[MasterProtocol] =
    Behaviors.receive { (context, message) =>
      message match {
        case WordCountTask(text, replyTo) => {
          workers.get(s"Worker$nextWorkerId") match {
            case Some(ref) =>
              context.log.info(
                s"Received word count task from $replyTo, sending task to $ref"
              )
              ref ! WorkerTask(nextWorkerId, text)
              Ready(
                nWorkers,
                (nextWorkerId + 1) % nWorkers,
                workers,
                users + (nextWorkerId -> replyTo)
              )
            case None =>
              context.log.error(
                s"Worker$nextWorkerId not found in the child actor pool. Doing Nothing"
              )
              Behaviors.same
          }
        }

        case WordCountReply(id, count) => {
          users.get(id) match {
            case Some(userRef) => {
              userRef ! Reply(count)
              Ready(nWorkers, nextWorkerId, workers, users - id)
            }
            case None =>
              context.log.info(s"User Not found for task with id: $id")
              Behaviors.same
          }
        }
      }

    }
}

object WordCountWorker {
  def apply(parent: ActorRef[MasterProtocol]): Behavior[WorkerProtocol] =
    Behaviors.receive { (context, message) =>
      message match {
        case WorkerTask(id, work) =>
          parent ! WordCountReply(id, work.split(" ").length)
          context.log.info(s"Received task from parent: $parent")
          Behaviors.same
      }
    }
}

object User {
  def apply(): Behavior[UserProtocol] = Behaviors.receive {
    (context, message) =>
      message match {
        case Reply(count) =>
          context.log.info(s"Received Reply: $count words")
          Behaviors.same
      }
  }
}

object ChildActorExercise {

  def apply(): Behavior[Nothing] = Behaviors.setup { (context) =>

    val userSystem = context.spawn(User(), "UserActor")

    val WorkerSystem = context.spawn(WordCountMaster(), "WordCountMaster")

    WorkerSystem ! Initiliaze(10)

    WorkerSystem ! WordCountTask(
      "This is the first message i'm sending",
      userSystem
    )

    WorkerSystem ! WordCountTask(
      "This is the second message i'm sending",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "This is the third message i'm sending, Long messages follow from here",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "A child actor can be forced to stop after it finishes processing its current message by using the stop method of the ActorContext from the parent actor. Only child actors can be stopped in that way.",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "The guardian actor should be responsible for initialization of tasks and create the initial actors of the application, but sometimes you might want to spawn new actors from the outside of the guardian actor. For example creating one actor per HTTP request.",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "The only simplification is that stopping a parent Actor will also recursively stop all the child Actors that this parent has created. All actors are also stopped automatically",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "An actor can create, or spawn, an arbitrary number of child actors, which in turn can spawn children of their own",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "An ActorSystem is a heavyweight structure that will allocate threads, so create one per logical application. Typically one ActorSystem per JVM process.",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation,",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "It is important to note that actors do not stop automatically when no longer referenced, every Actor that is created must also explicitly be destroyed.",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "The root actor is defined by the behavior used to create the ActorSystem",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "Must only be used in the ordinary actor message processing thread",
      userSystem
    )
    WorkerSystem ! WordCountTask(
      "The top level actor, also called the user guardian actor, is created along with the",
      userSystem
    )

    Behaviors.empty
  }

}

object Main extends App {
  val system = ActorSystem(ChildActorExercise(), "Exercise")
  Thread.sleep(5000)
  system.terminate()
}
