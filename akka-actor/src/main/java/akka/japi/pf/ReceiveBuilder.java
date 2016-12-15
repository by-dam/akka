/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.japi.pf;

import scala.PartialFunction;
import scala.runtime.BoxedUnit;
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;

/**
 * Used for building a partial function for {@link akka.actor.Actor#receive() Actor.receive()}.
 *
 * There is both a match on type only, and a match on type and predicate.
 *
 * Inside an actor you can use it like this with Java 8 to define your receive method.
 * <p>
 * Example:
 * </p>
 * <pre>
 * &#64;Override
 * public Actor() {
 *   receive(ReceiveBuilder.
 *     match(Double.class, d -&gt; {
 *       sender().tell(d.isNaN() ? 0 : d, self());
 *     }).
 *     match(Integer.class, i -&gt; {
 *       sender().tell(i * 10, self());
 *     }).
 *     match(String.class, s -&gt; s.startsWith("foo"), s -&gt; {
 *       sender().tell(s.toUpperCase(), self());
 *     }).build()
 *   );
 * }
 * </pre>
 *
 * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
 */
public class ReceiveBuilder {

  private PartialFunction<Object, BoxedUnit> statements = null;

  protected void addStatement(PartialFunction<Object, BoxedUnit> statement) {
    if (statements == null)
      statements = statement;
    else
      statements = statements.orElse(statement);
  }

  /**
   * Build a {@link scala.PartialFunction} from this builder. After this call
   * the builder will be reset.
   *
   * @return a PartialFunction for this builder.
   */
  public Receive build() {
    PartialFunction<Object, BoxedUnit> empty = CaseStatement.empty();

    statements = null;
    if (statements == null)
      return new Receive(empty);
    else
      return new Receive(statements.orElse(empty)); // FIXME why no new Receive(statements)?
  }

  /**
   * Return a new {@link ReceiveBuilder} with no case statements. They can be
   * added later as the returned {@link ReceiveBuilder} is a mutable object.
   *
   * @return a builder with no case statements
   */
  public static ReceiveBuilder create() {
    return new ReceiveBuilder();
  }

  // FIXME
  //  public static PartialFunction<Object, BoxedUnit> onMessage(FI.UnitApply<Object> apply) {
  //    return matchAny(apply).build();
  //  }

  /**
   * Add a new case statement to this builder.
   *
   * @param type
   *          a type to match the argument against
   * @param apply
   *          an action to apply to the argument if the type matches
   * @return a builder with the case statement added
   */
  @SuppressWarnings("unchecked")
  public <P> ReceiveBuilder match(final Class<? extends P> type, final FI.UnitApply<? extends P> apply) {

    FI.Predicate predicate = new FI.Predicate() {
      @Override
      public boolean defined(Object o) {
        return type.isInstance(o);
      }
    };

    addStatement(new UnitCaseStatement<Object, P>(predicate, (FI.UnitApply<P>) apply));

    return this;
  }

  /**
   * Add a new case statement to this builder.
   *
   * @param type
   *          a type to match the argument against
   * @param predicate
   *          a predicate that will be evaluated on the argument if the type
   *          matches
   * @param apply
   *          an action to apply to the argument if the type matches and the
   *          predicate returns true
   * @return a builder with the case statement added
   */
  @SuppressWarnings("unchecked")
  public <P> ReceiveBuilder match(final Class<? extends P> type, final FI.TypedPredicate<? extends P> predicate,
      final FI.UnitApply<? extends P> apply) {
    FI.Predicate fiPredicate = new FI.Predicate() {
      @Override
      public boolean defined(Object o) {
        if (!type.isInstance(o))
          return false;
        else {
          @SuppressWarnings("unchecked")
          P p = (P) o;
          return ((FI.TypedPredicate<P>) predicate).defined(p);
        }
      }
    };

    addStatement(new UnitCaseStatement<Object, P>(fiPredicate, (FI.UnitApply<P>) apply));

    return this;
  }

  /**
   * Add a new case statement to this builder.
   *
   * @param object
   *          the object to compare equals with
   * @param apply
   *          an action to apply to the argument if the object compares equal
   * @return a builder with the case statement added
   */
  public <P> ReceiveBuilder matchEquals(final P object, final FI.UnitApply<P> apply) {
    addStatement(new UnitCaseStatement<Object, P>(new FI.Predicate() {
      @Override
      public boolean defined(Object o) {
        return object.equals(o);
      }
    }, apply));
    return this;
  }

  /**
   * Add a new case statement to this builder.
   *
   * @param object
   *          the object to compare equals with
   * @param predicate
   *          a predicate that will be evaluated on the argument if the object
   *          compares equal
   * @param apply
   *          an action to apply to the argument if the object compares equal
   * @return a builder with the case statement added
   */
  public <P> ReceiveBuilder matchEquals(final P object, final FI.TypedPredicate<P> predicate,
      final FI.UnitApply<P> apply) {
    addStatement(new UnitCaseStatement<Object, P>(new FI.Predicate() {
      @Override
      public boolean defined(Object o) {
        if (!object.equals(o))
          return false;
        else {
          @SuppressWarnings("unchecked")
          P p = (P) o;
          return predicate.defined(p);
        }
      }
    }, apply));
    return this;
  }

  /**
   * Add a new case statement to this builder, that matches any argument.
   *
   * @param apply
   *          an action to apply to the argument
   * @return a builder with the case statement added
   */
  public ReceiveBuilder matchAny(final FI.UnitApply<Object> apply) {
    addStatement(new UnitCaseStatement<Object, Object>(new FI.Predicate() {
      @Override
      public boolean defined(Object o) {
        return true;
      }
    }, apply));
    return this;
  }

}
