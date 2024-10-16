defmodule KafkaEx.New.Structs.OffsetFetch.TopicOffsetsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.OffsetFetch.CommittedOffset
  alias KafkaEx.New.Structs.OffsetFetch.TopicOffsets

  describe "from_offset_fetch/1" do
    test "creates topic offsets with nested entities" do
      response = %{
        topic: "test-topic",
        partitions: [%{partition: 1, offset: 1, metadata: "metadata", error_code: 0}]
      }

      result = TopicOffsets.from_offset_fetch(response)

      assert result == %TopicOffsets{
               topic: "test-topic",
               committed_offsets: [
                 %TopicOffsets.PartitionOffset{
                   partition: 1,
                   offset: 1,
                   metadata: "metadata",
                   error_code: 0
                 }
               ]
             }
    end
  end
end
