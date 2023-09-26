package com.twalthr.flink.examples;

import org.apache.commons.compress.utils.Sets;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.twalthr.flink.examples.Example_15_Table_State_Machine.SummarizeOrder.StateMachine.State.CANCELLED;
import static com.twalthr.flink.examples.Example_15_Table_State_Machine.SummarizeOrder.StateMachine.State.CHANGED;
import static com.twalthr.flink.examples.Example_15_Table_State_Machine.SummarizeOrder.StateMachine.State.CHECKOUT;
import static com.twalthr.flink.examples.Example_15_Table_State_Machine.SummarizeOrder.StateMachine.State.CONFIRMED;
import static com.twalthr.flink.examples.Example_15_Table_State_Machine.SummarizeOrder.StateMachine.State.INITIALIZED;
import static com.twalthr.flink.examples.Example_15_Table_State_Machine.SummarizeOrder.StateMachine.State.INVALID;
import static com.twalthr.flink.examples.Example_15_Table_State_Machine.SummarizeOrder.StateMachine.State.PAID;

/** A more advanced example for showing the capabilities of an aggregate function. */
public class Example_15_Table_State_Machine {

  public static void main(String[] args) {
    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    env.getConfig().set("parallelism.default", "1"); // due to little data
    env.getConfig().set("table.local-time-zone", "UTC");

    env.createTemporaryTable("Orders", KafkaDescriptors.ORDERS_DESCRIPTOR);
    env.createTemporarySystemFunction("SummarizeOrder", SummarizeOrder.class);

    // Use SQL for expressing the running totals
    env.sqlQuery(
            "SELECT o_rowtime, o_id, SummarizeOrder(o_status) OVER (\n"
                + "    PARTITION BY o_id\n"
                + "    ORDER BY o_rowtime\n"
                + "    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                + "  ) AS status\n"
                + "FROM Orders\n"
                + "ORDER BY o_rowtime")
        .execute()
        .print();
  }

  /**
   * Stateful aggregation function for validating state transitions and return the current state.
   */
  public static class SummarizeOrder
      extends AggregateFunction<String, SummarizeOrder.StateMachine> {

    /** Stateful intermediate result. */
    public static class StateMachine {

      /** Value that is managed by Flink's state backend and thus checkpointed. */
      public String currentState = INITIALIZED.name();

      /** Possible states. */
      public enum State {
        INITIALIZED,
        CHECKOUT,
        PAID,
        CONFIRMED,
        CHANGED,
        CANCELLED,
        INVALID
      }

      /** Valid state transitions. */
      public static final Map<State, Set<State>> validTransitions = new HashMap<>();

      static {
        validTransitions.put(INITIALIZED, Sets.newHashSet(CHECKOUT));
        validTransitions.put(CHECKOUT, Sets.newHashSet(PAID));
        validTransitions.put(PAID, Sets.newHashSet(CONFIRMED));
        validTransitions.put(CONFIRMED, Sets.newHashSet(CHANGED, CANCELLED));
      }

      /** Evaluator of the state machine. */
      public void transition(String targetState) {
        Set<State> validTargets = validTransitions.get(State.valueOf(currentState));
        if (validTargets.contains(State.valueOf(targetState))) {
          currentState = targetState;
        } else {
          currentState = INVALID.name();
        }
      }
    }

    @Override
    public StateMachine createAccumulator() {
      return new StateMachine();
    }

    /** Digest event into state machine. */
    public void accumulate(StateMachine stateMachine, String name) {
      stateMachine.transition(name);
    }

    @Override
    public String getValue(StateMachine stateMachine) {
      return stateMachine.currentState;
    }
  }
}
