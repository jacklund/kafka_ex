defmodule KafkaEx.New.Structs.OffsetFetch.CommittedOffsetTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.OffsetFetch.CommittedOffset

  describe "from_offset_fetch/1" do
    test "creates committed offset with nested entities" do
      response = %{partition: 1, offset: 1, metadata: "metadata", error_code: 0}

      result = CommittedOffset.from_offset_fetch(response)

      assert result == %CommittedOffset{
               partition: 1,
               offset: 1,
               metadata: "metadata",
               error_code: 0
             }
    end
  end
end
