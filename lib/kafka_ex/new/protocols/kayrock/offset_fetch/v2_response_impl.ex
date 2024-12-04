defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Response, for: [Kayrock.OffsetFetch.V2.Response] do
  def parse_response(%{error_code: code}, _consumer_group) when is_integer(code) and code > 0 do
    {:error, Kayrock.ErrorCode.code_to_atom!(code)}
  end

  def parse_response(response, consumer_group) do
    {:ok, build_offset_fetch(response, consumer_group)}
  end

  defp build_offset_fetch(response, consumer_group) do
    KafkaEx.New.Structs.OffsetFetch.from_offset_fetch(consumer_group, response)
  end
end
