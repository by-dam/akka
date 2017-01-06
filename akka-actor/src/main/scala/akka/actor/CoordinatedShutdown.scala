/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.actor

import scala.concurrent.duration._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import akka.Done
import com.typesafe.config.Config
import scala.concurrent.duration.FiniteDuration
import scala.annotation.tailrec
import com.typesafe.config.ConfigFactory
import akka.pattern.after
import java.util.concurrent.TimeoutException
import scala.util.control.NonFatal
import akka.event.Logging
import akka.dispatch.ExecutionContexts
import java.util.concurrent.Executors

object CoordinatedShutdown extends ExtensionId[CoordinatedShutdown] with ExtensionIdProvider {
  override def get(system: ActorSystem): CoordinatedShutdown = super.get(system)

  override def lookup = CoordinatedShutdown

  override def createExtension(system: ExtendedActorSystem): CoordinatedShutdown = {
    val conf = system.settings.config.getConfig("akka.coordinated-shutdown")
    val phases = phasesFromConfig(conf)
    val terminateActorSystem = conf.getBoolean("terminate-actor-system")
    val coord = new CoordinatedShutdown(system, phases)
    if (terminateActorSystem) {
      coord.addTask(PhaseActorSystemTerminate) { () ⇒
        system.terminate().map(_ ⇒ Done)(ExecutionContexts.sameThreadExecutionContext)
      }
    }
    coord
  }

  val PhaseClusterLeave = "cluster-leave"
  val PhaseClusterShardingShutdownRegion = "cluster-sharding-shutdown-region"
  val PhaseClusterExiting = "cluster-exiting"
  val PhaseClusterExitingDone = "cluster-exiting-done"
  val PhaseClusterShutdown = "cluster-shutdown"
  val PhaseActorSystemTerminate = "actor-system-terminate"

  /**
   * INTERNAL API
   */
  private[akka] final case class Phase(dependsOn: Set[String], timeout: FiniteDuration, recover: Boolean)

  /**
   * INTERNAL API
   */
  private[akka] def phasesFromConfig(conf: Config): Map[String, Phase] = {
    import scala.collection.JavaConverters._
    val defaultTimeout = conf.getString("default-timeout")
    val phasesConf = conf.getConfig("phases")
    val defaultPhaseConfig = ConfigFactory.parseString(s"""
      timeout = $defaultTimeout
      recover = true
      depends-on = []
    """)
    phasesConf.root.unwrapped.asScala.toMap.map {
      case (k, _: java.util.Map[_, _]) ⇒
        val c = phasesConf.getConfig(k).withFallback(defaultPhaseConfig)
        val dependsOn = c.getStringList("depends-on").asScala.toSet
        val timeout = c.getDuration("timeout", MILLISECONDS).millis
        val recover = c.getBoolean("recover")
        k → Phase(dependsOn, timeout, recover)
      case (k, v) ⇒
        throw new IllegalArgumentException(s"Expected object value for [$k], got [$v]")
    }
  }

  /**
   * INTERNAL API: https://en.wikipedia.org/wiki/Topological_sorting
   */
  private[akka] def topologicalSort(phases: Map[String, Phase]): List[String] = {
    var result = List.empty[String]
    var unmarked = phases.keySet ++ phases.values.flatMap(_.dependsOn) // in case phase is not defined as key
    var tempMark = Set.empty[String] // for detecting cycles

    while (unmarked.nonEmpty) {
      depthFirstSearch(unmarked.head)
    }

    def depthFirstSearch(u: String): Unit = {
      if (tempMark(u))
        throw new IllegalArgumentException("Cycle detected in graph of phases. It must be a DAG. " +
          s"phase [$u] depends transitively on itself. All dependencies: $phases")
      if (unmarked(u)) {
        tempMark += u
        phases.get(u) match {
          case Some(Phase(dependsOn, _, _)) ⇒ dependsOn.foreach(depthFirstSearch)
          case None                         ⇒
        }
        unmarked -= u // permanent mark
        tempMark -= u
        result = u :: result
      }
    }

    result.reverse
  }

}

final class CoordinatedShutdown private[akka] (
  system: ExtendedActorSystem,
  phases: Map[String, CoordinatedShutdown.Phase]) extends Extension {
  import CoordinatedShutdown.Phase

  private val log = Logging(system, getClass)
  private val knownPhases = phases.keySet ++ phases.values.flatMap(_.dependsOn)
  private val orderedPhases = CoordinatedShutdown.topologicalSort(phases)
  private val tasks = new ConcurrentHashMap[String, Vector[() ⇒ Future[Done]]]
  private val runStarted = new AtomicBoolean(false)
  private val runPromise = Promise[Done]()

  /**
   * Add a task to a phase. It doesn't remove previously added tasks.
   * Tasks added to the same phase are executed in parallel without any
   * ordering assumptions. Next phase will not start until all tasks of
   * previous phase have been completed.
   *
   * Tasks should typically be registered as early as possible after system
   * startup. When running the coordinated shutdown tasks that have been registered
   * will be performed but tasks that are added too late will not be run.
   * It is possible to add a task to a later phase by a task in an earlier phase
   * and it will be performed.
   */
  @tailrec def addTask(phase: String)(task: () ⇒ Future[Done]): Unit = {
    require(
      knownPhases(phase),
      s"Unknown phase [$phase], known phases [$knownPhases]. " +
        "All phases (along with their optional dependencies) must be defined in configuration")
    val current = tasks.get(phase)
    if (current == null) {
      if (tasks.putIfAbsent(phase, Vector(task)) != null)
        addTask(phase)(task) // CAS failed, retry
    } else {
      if (!tasks.replace(phase, current, current :+ task))
        addTask(phase)(task) // CAS failed, retry
    }
  }

  def run(): Future[Done] = {
    if (runStarted.compareAndSet(false, true)) {
      import system.dispatcher
      val debugEnabled = log.isDebugEnabled
      def loop(remainingPhases: List[String]): Future[Done] = {
        remainingPhases match {
          case Nil ⇒ Future.successful(Done)
          case phase :: remaining ⇒
            val phaseResult = (tasks.get(phase) match {
              case null ⇒
                if (debugEnabled) log.debug("Performing phase [{}] with [0] tasks", phase)
                Future.successful(Done)
              case tasks ⇒
                if (debugEnabled) log.debug("Performing phase [{}] with [{}] tasks", phase, tasks.size)
                // note that tasks within same phase are performed in parallel
                val recoverEnabled = phases(phase).recover
                val result = Future.sequence(tasks.map { task ⇒
                  try {
                    val r = task.apply()
                    if (recoverEnabled) r.recover {
                      case NonFatal(e) ⇒
                        log.warning("Task failed in phase [{}]: {}", phase, e.getMessage)
                        Done
                    }
                    else r
                  } catch {
                    case NonFatal(e) ⇒
                      // in case task.apply throws
                      if (recoverEnabled) {
                        log.warning("Task failed in phase [{}]: {}", phase, e.getMessage)
                        Future.successful(Done)
                      } else
                        Future.failed(e)
                  }
                }).map(_ ⇒ Done)(ExecutionContexts.sameThreadExecutionContext)
                val timeout = phases(phase).timeout
                val deadline = Deadline.now + timeout
                val timeoutFut = after(timeout, system.scheduler) {
                  if (phase == CoordinatedShutdown.PhaseActorSystemTerminate && deadline.hasTimeLeft) {
                    // too early, i.e. triggered by system termination
                    result
                  } else if (result.isCompleted)
                    Future.successful(Done)
                  else if (recoverEnabled) {
                    log.warning("Coordinated shutdown phase [{}] timed out after {}", phase, timeout)
                    Future.successful(Done)
                  } else
                    Future.failed(
                      new TimeoutException(s"Coordinated shutdown phase [$phase] timed out after $timeout"))
                }
                Future.firstCompletedOf(List(result, timeoutFut))
            })
            if (remaining.isEmpty)
              phaseResult // avoid flatMap when system terminated in last phase
            else
              phaseResult.flatMap(_ ⇒ loop(remaining))
        }
      }
      val done = loop(orderedPhases)
      runPromise.completeWith(done)
    }
    runPromise.future
  }

  /**
   * The configured timeout for a given `phase`.
   * For example useful as timeout when actor `ask` requests
   * is used as a task.
   */
  def timeout(phase: String): FiniteDuration =
    phases.get(phase) match {
      case Some(Phase(_, timeout, _)) ⇒ timeout
      case None ⇒
        throw new IllegalArgumentException(s"Unknown phase [$phase]. All phases must be defined in configuration")
    }

}
