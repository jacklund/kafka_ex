defmodule KafkaEx.Integration.Test do
  use ExUnit.Case
  @moduletag :integration

  test "KafkaEx.Server starts on Application start up" do
    pid = Process.whereis(KafkaEx.Server)
    assert is_pid(pid)
  end

  #create_worker
  test "KafkaEx.Supervisor dynamically creates workers" do
    {:ok, pid} = KafkaEx.create_worker(:bar, uris)
    assert Process.whereis(:bar) == pid
  end

  test "KafkaEx.Server generates metadata on start up" do
    pid = Process.whereis(KafkaEx.Server)
    {metadata, _client, _} = :sys.get_state(pid)
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "start_link creates the server and registers it as the module name" do
    {:ok, pid} = KafkaEx.create_worker(:test_server, uris)
    assert pid == Process.whereis(:test_server)
  end

  # test "start_link raises an exception when it is provided a bad connection" do
  #   {:error, {exception, _}} = KafkaEx.create_worker(:no_host, [{"bad_host", 1000}])
  #   assert exception.__struct__ == KafkaEx.ConnectionError
  #   assert exception.message == "Error: Cannot connect to any of the broker(s) provided"
  # end

  #produce
  test "produce withiout an acq required returns :ok" do
    assert KafkaEx.produce("food", 0, "hey") == :ok
  end

  test "produce with ack required returns an ack" do
    {:ok, %{"food" => %{0 => %{error_code: 0, offset: offset}}}} =  KafkaEx.produce("food", 0, "hey", worker_name: KafkaEx.Server, required_acks: 1)
    refute offset == nil
  end

  test "produce updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    empty_metadata = KafkaEx.Metadata.new(uris)
    :sys.replace_state(pid, fn({_metadata, client, _}) -> {empty_metadata, client, nil} end)
    KafkaEx.produce("food", 0, "hey")
    {metadata, _client, _} = :sys.get_state(pid)
    refute metadata == empty_metadata

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "produce creates log for a non-existing topic" do
    random_string = TestHelper.generate_random_string
    KafkaEx.produce(random_string, 0, "hey")
    pid = Process.whereis(KafkaEx.Server)
    {metadata, _client, _} = :sys.get_state(pid)
    random_topic_metadata_found = metadata[:topics] |> Map.keys |> Enum.member?(random_string)

    assert random_topic_metadata_found
  end

  #metadata
  test "metadata for a non-existing topic creates a new topic" do
    random_string = TestHelper.generate_random_string
    random_topic_metadata = KafkaEx.metadata(topic: random_string)[:topics][random_string]
    assert random_topic_metadata[:error_code] == :no_error
    refute random_topic_metadata[:partitions] == %{}

    pid = Process.whereis(KafkaEx.Server)
    {metadata, _client, _} = :sys.get_state(pid)
    random_topic_metadata_found = metadata[:topics] |> Map.keys |> Enum.member?(random_string)

    assert random_topic_metadata_found
  end

  #fetch
  test "fetch updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    empty_metadata = KafkaEx.Metadata.new(uris)
    :sys.replace_state(pid, fn({_metadata, client, _}) -> {empty_metadata, client, nil} end)
    KafkaEx.fetch("food", 0, 0)
    {metadata, _client, _} = :sys.get_state(pid)
    refute metadata == empty_metadata

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "fetch does not blow up with incomplete bytes" do
    {:ok, map} = KafkaEx.fetch("food", 0, 0, max_bytes: 100)
  end

  test "fetch retrieves empty logs for non-exisiting topic" do
    random_string = TestHelper.generate_random_string
    {:ok, map} = KafkaEx.fetch(random_string, 0, 0)
    %{0 => %{message_set: message_set}} = Map.get(map, random_string)

    assert message_set == []
  end

  test "fetch works" do
    {:ok, %{"food" => %{0 => %{error_code: 0, offset: offset}}}} =  KafkaEx.produce("food", 0, "hey foo", worker_name: KafkaEx.Server, required_acks: 1)
    {:ok, %{"food" => %{0 => %{message_set: message_set}}}} = KafkaEx.fetch("food", 0, 0)
    message = message_set |> Enum.reverse |> hd

    assert message.value == "hey foo"
    assert message.offset == offset
  end

  #offset
  test "offset updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    empty_metadata = KafkaEx.Metadata.new(uris)
    :sys.replace_state(pid, fn({_metadata, client, _}) -> {empty_metadata, client, nil} end)
    KafkaEx.offset("food", 0, utc_time)
    {metadata, _client, _} = :sys.get_state(pid)
    refute metadata == empty_metadata

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "offset retrieves most recent offset by time specification" do
    random_string = TestHelper.generate_random_string
    KafkaEx.produce(random_string, 0, "hey")
    {:ok, map} = KafkaEx.offset(random_string, 0, utc_time)
    %{0 => %{offsets: [offset]}} = Map.get(map, random_string)

    assert offset != 0
  end

  test "earliest_offset retrieves offset of 0" do
    random_string = TestHelper.generate_random_string
    KafkaEx.produce(random_string, 0, random_string)
    {:ok, map} = KafkaEx.earliest_offset(random_string, 0)
    %{0 => %{offsets: [offset]}} = Map.get(map, random_string)

    assert offset == 0
  end

  test "latest_offset retrieves offset of 0 for non-existing topic" do
    random_string = TestHelper.generate_random_string
    {:ok, map} = KafkaEx.latest_offset(random_string, 0)
    %{0 => %{offsets: [offset]}} = Map.get(map, random_string)

    assert offset == 0
  end

  test "latest_offset retrieves a non-zero offset for a topic published to" do
    random_string = TestHelper.generate_random_string
    KafkaEx.produce(random_string, 0, "foo")
    {:ok, map} = KafkaEx.latest_offset(random_string, 0)
    %{0 => %{offsets: [offset]}} = Map.get(map, random_string)

    assert offset != 0
  end

  # stream
  test "streams kafka logs" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:stream, uris)
    KafkaEx.produce(random_string, 0, "hey", worker_name: :stream)
    KafkaEx.produce(random_string, 0, "hi", worker_name: :stream)
    log = KafkaEx.stream(random_string, 0, worker_name: :stream) |> Enum.take(2)

    refute Enum.empty?(log)
    [first,second|_] = log
    assert first.value == "hey"
    assert second.value == "hi"
  end

  test "stop_streaming stops streaming, and stream starts it up again" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:stream2, uris)
    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2)

    KafkaEx.create_worker(:producer, uris)
    KafkaEx.produce(random_string, 0, "one", worker_name: :producer)
    KafkaEx.produce(random_string, 0, "two", worker_name: :producer)

    :timer.sleep(1000)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 2
    last_offset = hd(Enum.reverse(log)).offset

    KafkaEx.stop_streaming(worker_name: :stream2)
    :timer.sleep(1000)
    KafkaEx.produce(random_string, 0, "three", worker_name: :producer)
    KafkaEx.produce(random_string, 0, "four", worker_name: :producer)
    :timer.sleep(1000)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 0

    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2, offset: last_offset+1)
    KafkaEx.produce(random_string, 0, "five", worker_name: :producer)
    KafkaEx.produce(random_string, 0, "six", worker_name: :producer)
    :timer.sleep(1000)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 4
  end

  def uris do
    Mix.Config.read!("config/config.exs") |> hd |> elem(1) |> hd |> elem(1)
  end

  def utc_time do
    {x, {a,b,c}} = :calendar.local_time |> :calendar.local_time_to_universal_time_dst |> hd
    {x, {a,b,c + 30}}
  end
end
