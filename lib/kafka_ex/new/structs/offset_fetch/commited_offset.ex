defmodule KafkaEx.New.Structs.OffsetFetch.CommittedOffset do
  @moduledoc """
  This module represents Committed Offset response coming from Kafka
  Committed Offset represents the last committed offset for a set of partitions in a consumer group.
  """
  defstruct [:partition, :offset, :metadata, :error_code]

  @type t :: %__MODULE__{
          partition: KafkaEx.Types.partition(),
          offset: KafkaEx.Types.offset(),
          metadata: KafkaEx.Types.metadata(),
          error_code: KafkaEx.Types.error_code()
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
