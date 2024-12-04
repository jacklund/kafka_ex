defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch do
  @moduledoc """
  This module implements the Offset Fetch protocol..
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Offset Fetch request
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Offset Fetch response
    """
    alias KafkaEx.New.Structs.Offset

    @spec parse_response(t(), KafkaEx.Types.consumer_group_name()) :: {:ok, [Offset.t()]} | {:error, term}
    def parse_response(response, consumer_group)
  end
end
