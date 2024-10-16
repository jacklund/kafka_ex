defmodule KafkaEx.New.Structs.OffsetFetch.CommittedOffset do
  @moduledoc """
  This module represents Committed Offset response coming from Kafka
  Committed Offset represents the last committed offset for a set of partitions in a consumer group.
  """
  defstruct [:partition, :offset, :metadata, :error_code]

  @type partition :: integer()
  @type offset :: integer()
  @type metadata :: String.t()
  @type error_code :: KafkaEx.error_code() | atom()

  @type t :: %__MODULE__{
          partition: partition,
          offset: offset,
          metadata: metadata,
          error_code: error_code
        }

  @spec from_offset_fetch(map) :: __MODULE__.t()
  def from_offset_fetch(resp) do
    %__MODULE__{
      partition: resp.partition,
      offset: resp.offset,
      metadata: resp.metadata,
      error_code: resp.error_code
    }
  end
end
