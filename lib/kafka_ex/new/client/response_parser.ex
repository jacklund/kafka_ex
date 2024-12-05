defmodule KafkaEx.New.Client.ResponseParser do
  @moduledoc """
  This module is used to parse response from KafkaEx.New.Client.
  It's main decision point which protocol to use for parsing response
  """
  alias KafkaEx.New.Structs.ConsumerGroup
  alias KafkaEx.New.Structs.Error
  alias KafkaEx.New.Structs.Offset

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.New.Protocols.KayrockProtocol)

  @doc """
  Parses response for Describe Groups API
  """
  def describe_groups_response(response, request) do
    @protocol.parse_response(:describe_groups, response, request)
  end

  @doc """
  Parses response for List Groups API
  """
  def list_offsets_response(response, request) do
    @protocol.parse_response(:list_offsets, response, request)
  end

  @doc """
  Parses response for Offset Fetch API
  """
  def offset_fetch_response(response, request) do
    @protocol.parse_response(:offset_fetch, response, request)
  end
end
