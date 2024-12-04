defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch.Request, as: OffsetFetchRequest

  alias Kayrock.OffsetFetch.V0
  alias Kayrock.OffsetFetch.V1
  alias Kayrock.OffsetFetch.V2
  alias Kayrock.OffsetFetch.V3

  describe "build_request/2" do
    test "for v0 - it builds offset fetch request" do
      request_template = %V0.Request{}
      opts = [consumer_group: "test-group", topics: [%{topic: "test-topic", partitions: [1, 2, 3]}]]

      request = OffsetFetchRequest.build_request(request_template, opts)

      assert request == %V0.Request{
               group_id: "test-group",
               topics: [
                 %{topic: "test-topic", partitions: [1, 2, 3]}
               ]
             }
    end

    test "for v1 - it builds offset fetch request" do
      request_template = %V1.Request{}
      opts = [consumer_group: "test-group", topics: [%{topic: "test-topic", partitions: [1, 2, 3]}]]

      request = OffsetFetchRequest.build_request(request_template, opts)

      assert request == %V1.Request{
               group_id: "test-group",
               topics: [
                 %{topic: "test-topic", partitions: [1, 2, 3]}
               ]
             }
    end

    test "for v2 - it builds offset fetch request" do
      request_template = %V2.Request{}
      opts = [consumer_group: "test-group", topics: [%{topic: "test-topic", partitions: [1, 2, 3]}]]

      request = OffsetFetchRequest.build_request(request_template, opts)

      assert request == %V2.Request{
               group_id: "test-group",
               topics: [
                 %{topic: "test-topic", partitions: [1, 2, 3]}
               ]
             }
    end

    test "for v3 - it builds offset fetch request" do
      request_template = %V3.Request{}
      opts = [consumer_group: "test-group", topics: [%{topic: "test-topic", partitions: [1, 2, 3]}]]

      request = OffsetFetchRequest.build_request(request_template, opts)

      assert request == %V3.Request{
               group_id: "test-group",
               topics: [
                 %{topic: "test-topic", partitions: [1, 2, 3]}
               ]
             }
    end
  end
end
