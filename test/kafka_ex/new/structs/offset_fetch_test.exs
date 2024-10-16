defmodule KafkaEx.New.Structs.OffsetFetchTest do
  use ExUnit.Case, async: true
  alias KafkaEx.New.Structs.OffsetFetch

  describe "from_offset_fetch/2" do
    test "creates offset fetch structure with nested entities" do
      consumer_group = "test-group"

      response = %{
        topics: [
          %{topic: "test-topic", partitions: [%{partition: 1, offset: 1, metadata: "metadata", error_code: 0}]}
        ]
      }

      result = OffsetFetch.from_offset_fetch(consumer_group, response)

      assert result == %OffsetFetch{
               consumer_group: consumer_group,
               topics: [
                 %OffsetFetch.TopicOffsets{
                   topic: "test-topic",
                   committed_offsets: [
                     %OffsetFetch.CommittedOffset{
                       partition: 1,
                       offset: 1,
                       metadata: "metadata",
                       error_code: 0
                     }
                   ]
                 }
               ]
             }
    end
  end
end
