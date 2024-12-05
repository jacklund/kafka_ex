defmodule KafkaEx.New.Client.Protocol do
  @moduledoc """
  This module is responsible for defining the behaviour of a protocol.
  """
  # ------------------------------------------------------------------------------
  @type opts :: Keyword.t()
  @type response :: term
  @type request :: term

  # ------------------------------------------------------------------------------
  @callback build_request(:describe_groups, KafkaEx.Types.api_version(), opts) :: request
  @callback build_request(:list_offsets, KafkaEx.Types.api_version(), opts) :: request
  @callback build_request(:offset_fetch, KafkaEx.Types.api_version(), opts) :: request

  # ------------------------------------------------------------------------------
  @type consumer_group :: KafkaEx.New.Structs.ConsumerGroup
  @type topic_offset :: KafkaEx.New.Structs.Offset
  @type offset_fetch :: KafkaEx.New.Structs.OffsetFetch

  @callback parse_response(:describe_groups, response, request) :: {:ok, [consumer_group]} | {:error, term}
  @callback parse_response(:list_offsets, response, request) :: {:ok, [topic_offset]} | {:error, term}
  @callback parse_response(:offset_fetch, response, request) :: {:ok, [offset_fetch]} | {:error, term}
end
