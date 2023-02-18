defmodule KafkaEx.New.Client.Test do
  use ExUnit.Case

  alias KafkaEx.New.Client

  alias KafkaEx.New.ClusterMetadata
  alias KafkaEx.New.KafkaExAPI
  alias KafkaEx.New.Topic
  alias KafkaEx.New.NodeSelector

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  @moduletag :new_client

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "update_metadata/0" do
    test "we don't fetch any topics on startup", %{client: client} do
      {:ok, updated_metadata} = GenServer.call(client, :update_metadata)
      %ClusterMetadata{topics: topics} = updated_metadata

      assert topics == %{}
    end
  end

  describe "topic_metadata/1" do
    test "returns topic metadata", %{client: client} do
      {:ok, [topic_metadata]} =
        GenServer.call(client, {:topic_metadata, ["test0p8p0"], false})

      assert %Topic{name: "test0p8p0"} = topic_metadata
    end
  end

  describe "list_offset/3" do
    test "list offset", %{client: client} do
      topic = "test0p8p0"

      for partition <- 0..3 do
        request = %Kayrock.ListOffsets.V1.Request{
          replica_id: -1,
          topics: [
            %{
              topic: topic,
              partitions: [%{partition: partition, timestamp: -1}]
            }
          ]
        }

        {:ok, resp} =
          Client.send_request(
            client,
            request,
            NodeSelector.topic_partition(topic, partition)
          )

        %Kayrock.ListOffsets.V1.Response{responses: responses} = resp
        [main_resp] = responses

        [%{error_code: error_code, offset: offset}] =
          main_resp.partition_responses

        assert error_code == 0

        {:ok, latest_offset} =
          KafkaExAPI.latest_offset(client, topic, partition)

        assert latest_offset == offset
      end
    end
  end

  describe "describe_groups/1" do
    setup do
      consumer_group = generate_random_string()
      topic = "test0p8p0"

      {:ok, pid} = KafkaEx.create_worker(:describe_groups, uris: uris(), consumer_group: consumer_group)

      on_exit(fn ->
        KafkaEx.delete_worker(:describe_groups)
      end)

      {:ok, %{consumer_group: consumer_group, topic: topic}}
    end

    test "returns group metadata for single consumer group", %{consumer_group: consumer_group, topic: topic} do
      KafkaEx.fetch(topic,0, offset: 0, worker_name: :describe_groups)

      {:ok, [group_metadata]} = GenServer.call(:baz, {:describe_groups, [consumer_group]})

      assert group_metadata.group_id == consumer_group
      assert group_metadata.protocol_type == "consumer"
      assert group_metadata.protocol == "consumer"
      assert group_metadata.members != []
    end

    test "returns error when consumer group request failed", %{consumer_group: consumer_group} do
      {:ok, [group_metadata]} =
        GenServer.call(:baz, {:describe_groups, ["non-existing-group"]})

      assert group_metadata.error_code == 25
    end
  end

  test "produce (new message format)", %{client: client} do
    topic = "test0p8p0"
    partition = 1

    {:ok, offset_before} = KafkaExAPI.latest_offset(client, topic, partition)

    record_batch = RecordBatch.from_binary_list(["foo", "bar", "baz"])

    request = %Kayrock.Produce.V1.Request{
      acks: -1,
      timeout: 1000,
      topic_data: [
        %{
          topic: topic,
          data: [
            %{partition: partition, record_set: record_batch}
          ]
        }
      ]
    }

    {:ok, response} =
      Client.send_request(
        client,
        request,
        NodeSelector.topic_partition(topic, partition)
      )

    %Kayrock.Produce.V1.Response{responses: [topic_response]} = response
    assert topic_response.topic == topic

    [%{partition: ^partition, error_code: error_code}] =
      topic_response.partition_responses

    assert error_code == 0

    {:ok, offset_after} = KafkaExAPI.latest_offset(client, topic, partition)
    assert offset_after == offset_before + 3
  end

  test "produce with record headers (new message format)", %{client: client} do
    topic = "test0p8p0"
    partition = 1

    {:ok, offset_before} = KafkaExAPI.latest_offset(client, topic, partition)

    headers = [
      %RecordHeader{key: "source", value: "System-X"},
      %RecordHeader{key: "type", value: "HeaderCreatedEvent"}
    ]

    records = [
      %Record{
        headers: headers,
        key: "key-0001",
        value: "msg value for key 0001"
      }
    ]

    record_batch = %RecordBatch{
      attributes: 0,
      records: records
    }

    request = %Kayrock.Produce.V1.Request{
      acks: -1,
      timeout: 1000,
      topic_data: [
        %{
          topic: topic,
          data: [
            %{partition: partition, record_set: record_batch}
          ]
        }
      ]
    }

    {:ok, response} =
      Client.send_request(
        client,
        request,
        NodeSelector.topic_partition(topic, partition)
      )

    %Kayrock.Produce.V1.Response{responses: [topic_response]} = response
    assert topic_response.topic == topic

    [%{partition: ^partition, error_code: error_code}] =
      topic_response.partition_responses

    assert error_code == 0

    {:ok, offset_after} = KafkaExAPI.latest_offset(client, topic, partition)
    assert offset_after == offset_before + 1
  end

  test "client can receive {:ssl_closed, _}", %{client: client} do
    send(client, {:ssl_closed, :unused})

    TestHelper.wait_for(fn ->
      {:message_queue_len, m} = Process.info(client, :message_queue_len)
      m == 0
    end)

    assert Process.alive?(client)
  end

  test "client can receive {:tcp_closed, _}", %{client: client} do
    send(client, {:tcp_closed, :unused})

    TestHelper.wait_for(fn ->
      {:message_queue_len, m} = Process.info(client, :message_queue_len)
      m == 0
    end)

    assert Process.alive?(client)
  end
end
