/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.actor

import akka.japi.pf.ReceiveBuilder
import akka.japi.pf.UnitPFBuilder
import scala.runtime.BoxedUnit

/**
 * Java API: compatible with lambda expressions
 *
 * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
 */
object AbstractActor {

  class Receive(val onMessage: PartialFunction[Any, BoxedUnit]) {
    def orElse(other: Receive): Receive = new Receive(onMessage.orElse(other.onMessage))
  }

  /**
   * emptyBehavior is a Receive-expression that matches no messages at all, ever.
   */
  final val emptyBehavior: Receive = new Receive(PartialFunction.empty)

  /**
   * The actor context - the view of the actor cell from the actor.
   * Exposes contextual information for the actor and the current message.
   */
  trait ActorContext extends akka.actor.ActorContext {

    /**
     * Returns an unmodifiable Java Collection containing the linked actors,
     * please note that the backing map is thread-safe but not immutable
     */
    def getChildren(): java.lang.Iterable[ActorRef]

    /**
     * Returns a reference to the named child or null if no child with
     * that name exists.
     */
    def getChild(name: String): ActorRef

    /**
     * Changes the Actor's behavior to become the new 'Receive' handler.
     * Replaces the current behavior on the top of the behavior stack.
     */
    def become(behavior: Receive): Unit =
      become(behavior, discardOld = true)

    /**
     * Changes the Actor's behavior to become the new 'Receive' handler.
     * This method acts upon the behavior stack as follows:
     *
     *  - if `discardOld = true` it will replace the top element (i.e. the current behavior)
     *  - if `discardOld = false` it will keep the current behavior and push the given one atop
     *
     * The default of replacing the current behavior on the stack has been chosen to avoid memory
     * leaks in case client code is written without consulting this documentation first (i.e.
     * always pushing new behaviors and never issuing an `unbecome()`)
     */
    def become(behavior: Receive, discardOld: Boolean): Unit =
      become(behavior.onMessage.asInstanceOf[PartialFunction[Any, Unit]], discardOld)
  }
}

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class that should be extended to create Java actors that use lambdas.
 * <p/>
 * Example:
 * <pre>
 * public class MyActor extends AbstractActor {
 *   int count = 0;
 *
 *   public MyActor() {
 *     receive(ReceiveBuilder.
 *       match(Double.class, d -> {
 *         sender().tell(d.isNaN() ? 0 : d, self());
 *       }).
 *       match(Integer.class, i -> {
 *         sender().tell(i * 10, self());
 *       }).
 *       match(String.class, s -> s.startsWith("foo"), s -> {
 *         sender().tell(s.toUpperCase(), self());
 *       }).build()
 *     );
 *   }
 * }
 * </pre>
 *
 * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
 */
abstract class AbstractActor extends Actor {

  /**
   * Returns this AbstractActor's ActorContext
   * The ActorContext is not thread safe so do not expose it outside of the
   * AbstractActor.
   */
  def getContext(): AbstractActor.ActorContext = context.asInstanceOf[AbstractActor.ActorContext]

  /**
   * Returns the ActorRef for this actor.
   */
  def getSelf(): ActorRef = self

  /**
   * The reference sender Actor of the currently processed message. This is
   * always a legal destination to send to, even if there is no logical recipient
   * for the reply, in which case it will be sent to the dead letter mailbox.
   */
  def getSender(): ActorRef = sender()

  /**
   * User overridable definition the strategy to use for supervising
   * child actors.
   */
  override def supervisorStrategy: SupervisorStrategy = super.supervisorStrategy

  /**
   * User overridable callback.
   * <p/>
   * Is called when an Actor is started.
   * Actor are automatically started asynchronously when created.
   * Empty default implementation.
   */
  @throws(classOf[Exception])
  override def preStart(): Unit = super.preStart()

  /**
   * User overridable callback.
   * <p/>
   * Is called asynchronously after 'actor.stop()' is invoked.
   * Empty default implementation.
   */
  @throws(classOf[Exception])
  override def postStop(): Unit = super.postStop()

  /**
   * User overridable callback: '''By default it disposes of all children and then calls `postStop()`.'''
   * <p/>
   * Is called on a crashed Actor right BEFORE it is restarted to allow clean
   * up of resources before Actor is terminated.
   */
  @throws(classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = super.preRestart(reason, message)

  /**
   * User overridable callback: By default it calls `preStart()`.
   * <p/>
   * Is called right AFTER restart on the newly created Actor to allow reinitialization after an Actor crash.
   */
  @throws(classOf[Exception])
  override def postRestart(reason: Throwable): Unit = super.postRestart(reason)

  def initialReceive(): AbstractActor.Receive

  override def receive: PartialFunction[Any, Unit] =
    initialReceive().onMessage.asInstanceOf[PartialFunction[Any, Unit]]

  final def receiveBuilder(): ReceiveBuilder = ReceiveBuilder.create()
}

abstract class UntypedAbstractActor extends AbstractActor {

  final override def initialReceive(): AbstractActor.Receive =
    throw new UnsupportedOperationException("initialReceive should not be used by UntypedAbstractActor")

  override def receive: PartialFunction[Any, Unit] = { case msg ⇒ onReceive(msg) }

  /**
   * To be implemented by concrete UntypedAbstractActor, this defines the behavior of the
   * UntypedActor.
   */
  @throws(classOf[Throwable])
  def onReceive(message: Any): Unit

  /**
   * Recommended convention is to call this method if the message
   * isn't handled in [[#onReceive]] (e.g. unknown message type).
   * By default it fails with either a [[akka.actor.DeathPactException]] (in
   * case of an unhandled [[akka.actor.Terminated]] message) or publishes an [[akka.actor.UnhandledMessage]]
   * to the actor's system's [[akka.event.EventStream]].
   */
  override def unhandled(message: Any): Unit = super.unhandled(message)
}

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class that mixes in logging into the Actor.
 *
 * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
 */
abstract class AbstractLoggingActor extends AbstractActor with ActorLogging

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class that should be extended to create an actor with a stash.
 *
 * The stash enables an actor to temporarily stash away messages that can not or
 * should not be handled using the actor's current behavior.
 * <p/>
 * Example:
 * <pre>
 * public class MyActorWithStash extends AbstractActorWithStash {
 *   int count = 0;
 *
 *   public MyActorWithStash() {
 *     receive(ReceiveBuilder. match(String.class, s -> {
 *       if (count < 0) {
 *         sender().tell(new Integer(s.length()), self());
 *       } else if (count == 2) {
 *         count = -1;
 *         unstashAll();
 *       } else {
 *         count += 1;
 *         stash();
 *       }}).build()
 *     );
 *   }
 * }
 * </pre>
 * Note that the subclasses of `AbstractActorWithStash` by default request a Deque based mailbox since this class
 * implements the `RequiresMessageQueue&lt;DequeBasedMessageQueueSemantics&gt;` marker interface.
 * You can override the default mailbox provided when `DequeBasedMessageQueueSemantics` are requested via config:
 * <pre>
 *   akka.actor.mailbox.requirements {
 *     "akka.dispatch.BoundedDequeBasedMessageQueueSemantics" = your-custom-mailbox
 *   }
 * </pre>
 * Alternatively, you can add your own requirement marker to the actor and configure a mailbox type to be used
 * for your marker.
 *
 * For a `Stash` based actor that enforces unbounded deques see [[akka.actor.AbstractActorWithUnboundedStash]].
 * There is also an unrestricted version [[akka.actor.AbstractActorWithUnrestrictedStash]] that does not
 * enforce the mailbox type.
 *
 * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
 */
abstract class AbstractActorWithStash extends AbstractActor with Stash

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class with `Stash` that enforces an unbounded deque for the actor. The proper mailbox has to be configured
 * manually, and the mailbox should extend the [[akka.dispatch.DequeBasedMessageQueueSemantics]] marker trait.
 * See [[akka.actor.AbstractActorWithStash]] for details on how `Stash` works.
 *
 * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
 */
abstract class AbstractActorWithUnboundedStash extends AbstractActor with UnboundedStash

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class with `Stash` that does not enforce any mailbox type. The mailbox of the actor has to be configured
 * manually. See [[akka.actor.AbstractActorWithStash]] for details on how `Stash` works.
 *
 * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
 */
abstract class AbstractActorWithUnrestrictedStash extends AbstractActor with UnrestrictedStash
