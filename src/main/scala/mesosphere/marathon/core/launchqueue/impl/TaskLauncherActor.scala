package mesosphere.marathon
package core.launchqueue.impl

import java.time.Clock

import akka.Done
import akka.actor._
import akka.event.LoggingReceive
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.flow.OfferReviver
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.{ InstanceChange, InstanceUpdateOperation }
import mesosphere.marathon.core.launcher.{ InstanceOp, InstanceOpFactory, OfferMatchResult }
import mesosphere.marathon.core.launchqueue.LaunchQueue.QueuedInstanceInfo
import mesosphere.marathon.core.launchqueue.LaunchQueueConfig
import mesosphere.marathon.core.launchqueue.impl.TaskLauncherActor.RecheckIfBackOffUntilReached
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.matcher.base.OfferMatcher.{ InstanceOpWithSource, MatchedInstanceOps }
import mesosphere.marathon.core.matcher.base.util.{ ActorOfferMatcher, InstanceOpSourceDelegate }
import mesosphere.marathon.core.matcher.manager.OfferMatcherManager
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{ Region, RunSpec, Timestamp }
import mesosphere.marathon.stream.Implicits._
import org.apache.mesos.{ Protos => Mesos }

import scala.concurrent.Promise
import scala.concurrent.duration._

private[launchqueue] object TaskLauncherActor {
  def props(
    config: LaunchQueueConfig,
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    taskOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    instanceTracker: InstanceTracker,
    rateLimiterActor: ActorRef,
    offerMatchStatisticsActor: ActorRef,
    localRegion: () => Option[Region])(
    runSpec: RunSpec): Props = {
    Props(new TaskLauncherActor(
      config,
      offerMatcherManager,
      clock, taskOpFactory,
      maybeOfferReviver,
      instanceTracker, rateLimiterActor, offerMatchStatisticsActor,
      runSpec, localRegion))
  }

  sealed trait Requests

  case class Sync(runSpec: RunSpec) extends Requests

  /**
    * Get the current count.
    * The actor responds with a [[QueuedInstanceInfo]] message.
    */
  case object GetCount extends Requests

  /**
    * Results in rechecking whether we may launch tasks.
    */
  private case object RecheckIfBackOffUntilReached extends Requests

  case object Stop extends Requests

  private val OfferOperationRejectedTimeoutReason: String =
    "InstanceLauncherActor: no accept received within timeout. " +
      "You can reconfigure the timeout with --task_operation_notification_timeout."
}

/**
  * Allows processing offers for starting tasks for the given app.
  */
private class TaskLauncherActor(
    config: LaunchQueueConfig,
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    instanceOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    instanceTracker: InstanceTracker,
    rateLimiterActor: ActorRef,
    offerMatchStatisticsActor: ActorRef,

    private[this] var runSpec: RunSpec,
    localRegion: () => Option[Region]) extends Actor with StrictLogging with Stash {
  // scalastyle:on parameter.number

  /** instances that are in the tracker */
  private[this] var instanceMap: Map[Instance.Id, Instance] = _

  private[this] def inFlightInstanceOperations = instanceMap.values.filter(_.state.condition == Condition.Provisioned)

  def scheduledInstances: Iterable[Instance] = instanceMap.values.filter(_.state.condition == Condition.Scheduled)
  def instancesToLaunch = scheduledInstances.size

  private[this] var recheckBackOff: Option[Cancellable] = None
  private[this] var backOffUntil: Option[Timestamp] = None

  /** Decorator to use this actor as a [[OfferMatcher#TaskOpSource]] */
  private[this] val myselfAsLaunchSource = InstanceOpSourceDelegate(self)

  private[this] val startedAt = clock.now()

  override def preStart(): Unit = {
    super.preStart()

    instanceMap = instanceTracker.instancesBySpecSync.instancesMap(runSpec.id).instanceMap

    logger.info(s"Started instanceLaunchActor for ${runSpec.id} version ${runSpec.version} with initial count $instancesToLaunch")
    rateLimiterActor ! RateLimiterActor.GetDelay(runSpec)
  }

  override def postStop(): Unit = {
    OfferMatcherRegistration.unregister()
    recheckBackOff.foreach(_.cancel())

    if (inFlightInstanceOperations.nonEmpty) {
      logger.warn(s"Actor shutdown while instances are in flight: ${inFlightInstanceOperations.map(_.instanceId).mkString(", ")}")
    }

    offerMatchStatisticsActor ! OfferMatchStatisticsActor.LaunchFinished(runSpec.id)

    super.postStop()

    logger.info(s"Stopped InstanceLauncherActor for ${runSpec.id} version ${runSpec.version}")
  }

  override def receive: Receive = waitForInitialDelay

  private[this] def waitForInitialDelay: Receive = LoggingReceive.withLabel("waitingForInitialDelay") {
    case RateLimiterActor.DelayUpdate(spec, delayUntil) if spec == runSpec =>
      logger.info(s"Got delay update for run spec ${spec.id}")
      stash()
      unstashAll()
      context.become(active)
    case msg @ RateLimiterActor.DelayUpdate(spec, delayUntil) if spec != runSpec =>
      logger.warn(s"Received delay update for other runSpec: $msg")
    case message: Any => stash()
  }

  private[this] def active: Receive = LoggingReceive.withLabel("active") {
    Seq(
      receiveStop,
      receiveSync,
      receiveDelayUpdate,
      receiveInstanceUpdate,
      receiveGetCurrentCount,
      receiveProcessOffers,
      receiveUnknown
    ).reduce(_.orElse[Any, Unit](_))
  }

  private[this] def receiveUnknown: Receive = {
    case msg: Any =>
      // fail fast and do not let the sender time out
      sender() ! Status.Failure(new IllegalStateException(s"Unhandled message: $msg"))
  }

  private[this] def receiveStop: Receive = {
    case TaskLauncherActor.Stop =>
      if (inFlightInstanceOperations.nonEmpty) {
        val taskIds = inFlightInstanceOperations.take(3).map(_.instanceId).mkString(", ")
        logger.info(
          s"Still waiting for ${inFlightInstanceOperations.size} inflight messages but stopping anyway. " +
            s"First three task ids: $taskIds"
        )
      }
      context.stop(self)
  }

  /**
    * Update internal instance map.
    */
  private[this] def receiveSync: Receive = {
    case TaskLauncherActor.Sync(newRunSpec) =>
      if (runSpec.isUpgrade(newRunSpec)) {
        logger.info(s"Received new run spec for ${newRunSpec.id} with version ${newRunSpec.version}")

        runSpec = newRunSpec // Sideeffect for suspendMatchingUntilWeGetBackoffDelayUpdate
        suspendMatchingUntilWeGetBackoffDelayUpdate()
      }
      instanceMap = instanceTracker.instancesBySpecSync.instancesMap(runSpec.id).instanceMap

      OfferMatcherRegistration.manageOfferMatcherStatus()
      replyWithQueuedInstanceCount()
  }

  /**
    * Receive rate limiter updates.
    */
  private[this] def receiveDelayUpdate: Receive = {
    case RateLimiterActor.DelayUpdate(spec, delayUntil) if spec == runSpec =>

      if (!backOffUntil.contains(delayUntil)) {

        backOffUntil = Some(delayUntil)

        recheckBackOff.foreach(_.cancel())
        recheckBackOff = None

        val now: Timestamp = clock.now()
        if (backOffUntil.exists(_ > now)) {
          import context.dispatcher
          recheckBackOff = Some(
            context.system.scheduler.scheduleOnce(now until delayUntil, self, RecheckIfBackOffUntilReached)
          )
        }

        OfferMatcherRegistration.manageOfferMatcherStatus()
      }

      logger.debug(s"After delay update $status")

    case msg @ RateLimiterActor.DelayUpdate(spec, delayUntil) if spec != runSpec =>
      logger.warn(s"Received delay update for other runSpec: $msg")

    case RecheckIfBackOffUntilReached => OfferMatcherRegistration.manageOfferMatcherStatus()
  }

  private[this] def receiveInstanceUpdate: Receive = {
    case change: InstanceChange =>
      instanceMap = instanceTracker.instancesBySpecSync.instancesMap(runSpec.id).instanceMap
      OfferMatcherRegistration.manageOfferMatcherStatus()
      sender() ! Done
  }

  private[this] def receiveGetCurrentCount: Receive = {
    case TaskLauncherActor.GetCount =>
      replyWithQueuedInstanceCount()
  }

  private[this] def suspendMatchingUntilWeGetBackoffDelayUpdate(): Unit = {
    // signal no interest in new offers until we get the back off delay.
    // this makes sure that we see unused offers again that we rejected for the old configuration.
    OfferMatcherRegistration.unregister()

    // get new back off delay, don't do anything until we get that.
    backOffUntil = None
    rateLimiterActor ! RateLimiterActor.GetDelay(runSpec)
    context.become(waitForInitialDelay)
  }

  private[this] def replyWithQueuedInstanceCount(): Unit = {
    val activeInstances = instanceMap.values.count(instance => instance.isActive || instance.isReserved)
    val instanceLaunchesInFlight = inFlightInstanceOperations.size
    sender() ! QueuedInstanceInfo(
      runSpec,
      inProgress = instancesToLaunch > 0 || inFlightInstanceOperations.nonEmpty,
      instancesLeftToLaunch = instancesToLaunch,
      finalInstanceCount = instancesToLaunch + instanceLaunchesInFlight + activeInstances,
      backOffUntil.getOrElse(clock.now()),
      startedAt
    )
  }

  private[this] def receiveProcessOffers: Receive = {
    case ActorOfferMatcher.MatchOffer(offer, promise) if !shouldLaunchInstances =>
      logger.debug(s"Ignoring offer ${offer.getId.getValue}: $status")
      promise.trySuccess(MatchedInstanceOps.noMatch(offer.getId))

    case ActorOfferMatcher.MatchOffer(offer, promise) =>
      logger.debug(s"Matching offer ${offer.getId} and need to launch $instancesToLaunch tasks.")
      val reachableInstances = instanceMap.filterNotAs{ case (_, instance) => instance.state.condition.isLost }
      val matchRequest = InstanceOpFactory.Request(runSpec, offer, reachableInstances, scheduledInstances, localRegion())
      instanceOpFactory.matchOfferRequest(matchRequest) match {
        case matched: OfferMatchResult.Match =>
          logger.debug(s"Matched offer ${offer.getId} for run spec ${runSpec.id}, ${runSpec.version}.")
          offerMatchStatisticsActor ! matched
          handleInstanceOp(matched.instanceOp, offer, promise)
        case notMatched: OfferMatchResult.NoMatch =>
          logger.debug(s"Did not match offer ${offer.getId} for run spec ${runSpec.id}, ${runSpec.version}.")
          offerMatchStatisticsActor ! notMatched
          promise.trySuccess(MatchedInstanceOps.noMatch(offer.getId))
      }
  }

  /**
    * Mutate internal state in response to having matched an instanceOp.
    *
    * @param instanceOp The instanceOp that is to be applied to on a previously
    *     received offer
    * @param offer The offer that could be matched successfully.
    * @param promise Promise that tells offer matcher that the offer has been accepted.
    */
  private[this] def handleInstanceOp(instanceOp: InstanceOp, offer: Mesos.Offer, promise: Promise[MatchedInstanceOps]): Unit = {

    // Mark instance in internal map as provisioned
    instanceOp.stateOp match {
      case InstanceUpdateOperation.Provision(instance) =>
        instanceMap += instance.instanceId -> instance
        logger.debug(s"Updated instance map to ${instanceMap.values.map(i => i.instanceId -> i.state.condition)}")
      case other =>
        logger.debug(s"Unexpected updated operation $other")
    }

    OfferMatcherRegistration.manageOfferMatcherStatus()

    logger.debug(s"Request ${instanceOp.getClass.getSimpleName} for instance '${instanceOp.instanceId.idString}', version '${runSpec.version}'. $status")
    promise.trySuccess(MatchedInstanceOps(offer.getId, Seq(InstanceOpWithSource(myselfAsLaunchSource, instanceOp))))
  }

  private[this] def scheduleTaskOpTimeout(instanceOp: InstanceOp): Unit = {
    val reject = InstanceOpSourceDelegate.InstanceOpRejected(
      instanceOp, TaskLauncherActor.OfferOperationRejectedTimeoutReason
    )
    val cancellable = scheduleTaskOperationTimeout(context, reject)
  }

  protected def scheduleTaskOperationTimeout(
    context: ActorContext,
    message: InstanceOpSourceDelegate.InstanceOpRejected): Cancellable =
    {
      import context.dispatcher
      context.system.scheduler.scheduleOnce(config.taskOpNotificationTimeout().milliseconds, self, message)
    }

  private[this] def backoffActive: Boolean = backOffUntil.forall(_ > clock.now())
  private[this] def shouldLaunchInstances: Boolean = instancesToLaunch > 0 && !backoffActive

  private[this] def status: String = {
    val backoffStr = backOffUntil match {
      case Some(until) if until > clock.now() => s"currently waiting for backoff($until)"
      case _ => "not backing off"
    }

    val inFlight = inFlightInstanceOperations.size
    val activeInstances = instanceMap.values.count(_.isActive) - inFlight
    val instanceCountDelta = instanceMap.size + instancesToLaunch - runSpec.instances
    val matchInstanceStr = if (instanceCountDelta == 0) "" else s"instance count delta $instanceCountDelta."
    s"$instancesToLaunch instancesToLaunch, $inFlight in flight, " +
      s"$activeInstances confirmed. $matchInstanceStr $backoffStr"
  }

  /** Manage registering this actor as offer matcher. Only register it if instancesToLaunch > 0. */
  private[this] object OfferMatcherRegistration {
    private[this] val myselfAsOfferMatcher: OfferMatcher = {
      //set the precedence only, if this app is resident
      new ActorOfferMatcher(self, if (runSpec.isResident) Some(runSpec.id) else None)
    }
    private[this] var registeredAsMatcher = false

    /** Register/unregister as necessary */
    def manageOfferMatcherStatus(): Unit = {
      val shouldBeRegistered = shouldLaunchInstances

      if (shouldBeRegistered && !registeredAsMatcher) {
        logger.debug(s"Registering for ${runSpec.id}, ${runSpec.version}.")
        offerMatcherManager.addSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = true
      } else if (!shouldBeRegistered && registeredAsMatcher) {
        if (instancesToLaunch > 0) {
          logger.info(s"Backing off due to task failures. Stop receiving offers for ${runSpec.id}, ${runSpec.version}")
        } else {
          logger.info(s"No tasks left to launch. Stop receiving offers for ${runSpec.id}, ${runSpec.version}")
        }
        offerMatcherManager.removeSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = false
      }
    }

    def unregister(): Unit = {
      if (registeredAsMatcher) {
        logger.info("Deregister as matcher.")
        offerMatcherManager.removeSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = false
      }
    }
  }
}
