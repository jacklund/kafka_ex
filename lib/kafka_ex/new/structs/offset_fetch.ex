defmodule KafkaEx.New.Structs.OffsetFetch do
  @moduledoc """
  This module represents Offset Fetch response coming from Kafka
  Offset Fetch represents the last committed offset for a set of partitions in a consumer group.
  """
  defstruct [:consumer_group, :topics]

  alias KafkaEx.New.Structs.OffsetFetch.TopicOffsets

  @type consumer_group :: String.t()
  @type t :: %__MODULE__{
          consumer_group: KafkaEx.Types.consumer_group_name(),
          topics: [TopicOffsets.t()]
        }

  @spec from_offset_fetch(consumer_group, map) :: __MODULE__.t()
  def from_offset_fetch(consumer_group, response) do
    %__MODULE__{
      consumer_group: consumer_group,
      topics: Enum.map(response.responses, &TopicOffsets.from_offset_fetch/1)
    }
  end
end
