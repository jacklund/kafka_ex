defmodule KafkaEx.Protocol.OffsetCommit.V1 do
  alias KafkaEx.Protocol

  @moduledoc """
  Implementation of the Kafka OffsetCommit request and response APIs
  """

  defmodule Request do
    @moduledoc false
    defstruct consumer_group: nil,
              topic:          nil,
              partition:      nil,
              offset:         nil,
              metadata:       "",
              member_id:      "",
              timestamp:      -1,
              generation_id:  -1,
              api_version:    1

    @type t :: %Request{
            consumer_group: binary,
            topic:          binary,
            partition:      integer,
            offset:         integer
          }
  end

  defmodule Response do
    @moduledoc false
    defstruct partitions: [], topic: nil
    @type t :: %Response{partitions: [] | [integer], topic: binary}
  end

  # Taken from https://kafka.apache.org/protocol#The_Messages_OffsetCommit
  @spec create_request(integer, binary, Request.t()) :: binary
  def create_request(correlation_id, client_id, offset_commit_request) do
    Protocol.create_request(:offset_commit, correlation_id, client_id, 1) <>
      <<byte_size(offset_commit_request.consumer_group)::16-signed,
        offset_commit_request.consumer_group::binary,
        offset_commit_request.generation_id::32-signed,
        byte_size(offset_commit_request.member_id)::16-signed,
        offset_commit_request.member_id::binary,
        1::32-signed,
          byte_size(offset_commit_request.topic)::16-signed,
          offset_commit_request.topic::binary,
          1::32-signed,
            offset_commit_request.partition::32-signed,
            offset_commit_request.offset::64,
            offset_commit_request.timestamp::64,
            byte_size(offset_commit_request.metadata)::16-signed,
            offset_commit_request.metadata::binary>>
  end

  @spec parse_response(binary) :: [] | [Response.t()]
  def parse_response(
        <<_correlation_id::32-signed, topics_count::32-signed,
          topics_data::binary>>
      ) do
    parse_topics(topics_count, topics_data)
  end

  defp parse_topics(0, _), do: []

  defp parse_topics(
         topic_count,
         <<topic_size::16-signed, topic::size(topic_size)-binary,
           partitions_count::32-signed, rest::binary>>
       ) do
    {partitions, topics_data} = parse_partitions(partitions_count, rest, [])

    [
      %Response{topic: topic, partitions: partitions}
      | parse_topics(topic_count - 1, topics_data)
    ]
  end

  defp parse_topics(_, _), do: []

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(
         partitions_count,
         <<partition::32-signed, _error_code::16-signed, rest::binary>>,
         partitions
       ) do
    # do something with error_code
    parse_partitions(partitions_count - 1, rest, [partition | partitions])
  end
end