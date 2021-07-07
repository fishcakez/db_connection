defmodule TestPoolDisconnectAll do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "disconnect on checkin" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :new_state1},
      {:ok, %Q{}, %R{}, :new_state2},
      {:ok, :dead_state},
      {:ok, :final_state},
      {:ok, %Q{}, %R{}, :final_state1},
      {:ok, %Q{}, %R{}, :final_state2}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}

    P.disconnect_all(pool, 0)
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}

    err = %DBConnection.ConnectionError{message: "disconnect_all requested", severity: :info}

    assert [
             connect: [_],
             handle_execute: [_, _, _, :state],
             handle_execute: [_, _, _, :new_state1],
             disconnect: [^err, :new_state2],
             connect: [_],
             handle_execute: [_, _, _, :final_state],
             handle_execute: [_, _, _, :final_state1]
           ] = A.record(agent)
  end

  @tag :idle_interval
  test "disconnect on ping" do
    parent = self()

    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :new_state},
      fn _, _ ->
        send(parent, :disconnecting)
        {:ok, :dead_state}
      end,
      {:ok, :final_state},
      {:ok, %Q{}, %R{}, :final_state1},
      {:ok, %Q{}, %R{}, :final_state2},
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), idle_interval: 50]
    {:ok, pool} = P.start_link(opts)
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}

    # High intervals do not affect ping because those are always disconnected.
    P.disconnect_all(pool, 45_000)
    assert_receive :disconnecting
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}

    err = %DBConnection.ConnectionError{message: "disconnect_all requested", severity: :info}

    assert [
             connect: [_],
             handle_execute: [_, _, _, :state],
             disconnect: [^err, :new_state],
             connect: [_],
             handle_execute: [_, _, _, :final_state],
             handle_execute: [_, _, _, :final_state1]
           ] = A.record(agent)
  end
end
