defmodule KafkaEx.Protocol.Fetch do
  def create_request_fn(topic, partition, offset, wait_time, min_bytes, max_bytes) do
    fn(correlation_id, client_id) ->
      KafkaEx.Protocol.create_request(:fetch, correlation_id, client_id) <>
        << -1 :: 32, wait_time :: 32, min_bytes :: 32, 1 :: 32, byte_size(topic) :: 16, topic :: binary,
           1 :: 32, partition :: 32, offset :: 64, max_bytes :: 32 >>
    end
  end

  def create_request(correlation_id, client_id, topic, partition, offset, wait_time, min_bytes, max_bytes) do
    KafkaEx.Protocol.create_request(:fetch, correlation_id, client_id) <>
      << -1 :: 32, wait_time :: 32, min_bytes :: 32, 1 :: 32, byte_size(topic) :: 16, topic :: binary,
         1 :: 32, partition :: 32, offset :: 64, max_bytes :: 32 >>
  end

  def parse_response(<< _correlation_id :: 32, num_topics :: 32, rest :: binary>>) do
    parse_topics(%{}, num_topics, rest)
    |> generate_result
  end

  defp generate_result({:ok, response_map, _rest}) do
    {:ok, response_map}
  end

  defp parse_topics(map, 0, rest) do
    {:ok, map, rest}
  end

  defp parse_topics(map, num_topics, << topic_size :: 16, topic :: size(topic_size)-binary,
                                        num_partitions :: 32, rest :: binary >>) do
    case parse_partitions(%{}, num_partitions, rest) do
      {:ok, partition_map, rest} ->
        parse_topics(Map.put(map, topic, partition_map), num_topics-1, rest)
    end
  end

  defp parse_partitions(map, 0, rest) do
    {:ok, map, rest}
  end

  defp parse_partitions(map, num_partitions,
                        << partition :: 32, error_code :: 16, hw_mark_offset :: 64,
                           msg_set_size :: 32, msg_set_data :: size(msg_set_size)-binary,
                           rest :: binary >>) do
    {:ok, message_set} = KafkaEx.Util.parse_message_set([], msg_set_data)
    parse_partitions(
      Map.put(map, partition,
        %{
          :error_code => error_code,
          :hw_mark_offset => hw_mark_offset,
          :message_set => message_set
        }
      ), num_partitions-1, rest
    )
  end
end
