defmodule KafkaEx.Protocol.Produce do
  defmodule Request do
    @moduledoc """
    - require_acks: indicates how many acknowledgements the servers should receive before responding to the request. If it is 0 the server will not send any response (this is the only case where the server will not reply to a request). If it is 1, the server will wait the data is written to the local log before sending a response. If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more acknowledgements than there are in-sync replicas), default is 0
    - timeout: provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks, default is 100 milliseconds
    """
    defstruct topic: nil, partition: nil, required_acks: 0, timeout: 0, compression: :none, messages: []
    @type t :: %Request{topic: binary, partition: integer, required_acks: binary, timeout: integer, compression: atom, messages: list}
  end

  defmodule Message do
    @moduledoc """
    - key: is used for partition assignment, can be nil, when none is provided it is defaulted to nil
    - value: is the message to be written to kafka logs.
    """
    defstruct key: nil, value: nil
    @type t :: %Message{key: binary, value: binary}
  end

  defmodule Response do
    defstruct topic: nil, partitions: []
    @type t :: %Response{topic: binary, partitions: list}
  end

  def create_request(correlation_id, client_id, %Request{topic: topic, partition: partition, required_acks: required_acks, timeout: timeout, compression: compression, messages: messages}) do
    message_set = create_message_set(messages, compression)
    KafkaEx.Protocol.create_request(:produce, correlation_id, client_id) <>
      << required_acks :: 16-signed, timeout :: 32-signed, 1 :: 32-signed >> <>
      << byte_size(topic) :: 16-signed, topic :: binary, 1 :: 32-signed, partition :: 32-signed, byte_size(message_set) :: 32-signed >> <> message_set
  end

  def parse_response(<< _correlation_id :: 32-signed, num_topics :: 32-signed, rest :: binary >>), do: parse_topics(num_topics, rest)
  def parse_response(unknown), do: unknown

  defp create_message_set([], _compression_type), do: ""
  defp create_message_set([%Message{key: key, value: value}|messages], :none) do
    message = create_message(value, key)
    message_set = << 0 :: 64-signed >> <> << byte_size(message) :: 32-signed >> <> message
    message_set <> create_message_set(messages, :none)
  end
  defp create_message_set(messages, compression_type) do
    message_set = create_message_set(messages, :none)
    {compressed_message_set, attribute} =
      KafkaEx.Compression.compress(compression_type, message_set)
    message = create_message(compressed_message_set, nil, attribute)

    << 0 :: 64-signed >> <> << byte_size(message) :: 32-signed >> <> message
  end

  defp create_message(value, key, attributes \\ 0) do
    sub = << 0 :: 8, attributes :: 8-signed >> <> bytes(key) <> bytes(value)
    crc = :erlang.crc32(sub)
    << crc :: 32 >> <> sub
  end

  defp bytes(nil), do: << -1 :: 32-signed >>
  defp bytes(data) do
    case byte_size(data) do
      0 -> << 0 :: 32 >>
      size -> << size :: 32, data :: binary >>
    end
  end

  defp parse_topics(0, _), do: []
  defp parse_topics(topics_size, << topic_size :: 16-signed, topic :: size(topic_size)-binary, partitions_size :: 32-signed, rest :: binary >>) do
    {partitions, topics_data} = parse_partitions(partitions_size, rest, [])
    [%Response{topic: topic, partitions: partitions} | parse_topics(topics_size - 1, topics_data)]
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}
  defp parse_partitions(partitions_size, << partition :: 32-signed, error_code :: 16-signed, offset :: 64, rest :: binary >>, partitions) do
    parse_partitions(partitions_size-1, rest, [%{partition: partition, error_code: error_code, offset: offset} | partitions])
  end

end
