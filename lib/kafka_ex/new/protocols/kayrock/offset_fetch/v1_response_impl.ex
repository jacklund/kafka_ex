defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Response, for: [Kayrock.OffsetFetch.V1.Response] do
  def parse_response(response, consumer_group) do
    {:ok, build_offset_fetch(response, consumer_group)}
  end

  defp build_offset_fetch(response, consumer_group) do
    KafkaEx.New.Structs.OffsetFetch.from_offset_fetch(consumer_group, response)
  end
end
