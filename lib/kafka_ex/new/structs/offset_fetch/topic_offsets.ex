defmodule KafkaEx.New.Structs.OffsetFetch.TopicOffsets do
  @moduledoc """
  This module represents Topic offsets list
  """
  defstruct [:topic, :committed_offsets]

  alias KafkaEx.New.Structs.OffsetFetch.CommittedOffset

  @type topic :: String.t()

  @type t :: %__MODULE__{
          topic: topic,
          committed_offsets: [CommittedOffset.t()]
        }

  @spec from_offset_fetch(map) :: __MODULE__.t()
  def from_offset_fetch(resp) do
    %__MODULE__{
      topic: resp.topic,
      committed_offsets: Enum.map(resp.partitions, &CommittedOffset.from_offset_fetch/1)
    }
  end
end
