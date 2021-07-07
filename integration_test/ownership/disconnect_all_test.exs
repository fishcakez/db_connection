defmodule TestOwnershipDisconnectAll do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R
  alias DBConnection.Ownership

  test "disconnect on checkin" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :new_state1},
      {:ok, %Q{}, %R{}, :new_state2},
      {:ok, %Q{}, %R{}, :new_state3},
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

    assert Ownership.ownership_checkin(pool, []) == :ok
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}

    err = %DBConnection.ConnectionError{message: "disconnect_all requested", severity: :info}

    assert [
             connect: [_],
             handle_execute: [_, _, _, :state],
             handle_execute: [_, _, _, :new_state1],
             handle_execute: [_, _, _, :new_state2],
             disconnect: [^err, :new_state3],
             connect: [_],
             handle_execute: [_, _, _, :final_state],
             handle_execute: [_, _, _, :final_state1]
           ] = A.record(agent)
  end
end
