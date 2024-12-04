defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Request, for: [Kayrock.OffsetFetch.V2.Request] do
  def build_request(request_template, opts) do
    topics = Keyword.fetch!(opts, :topics)

    Map.merge(request_template, %{
      group_id: Keyword.fetch!(opts, :consumer_group),
      topics:
        Enum.map(topics, fn topic ->
          %{
            topic: topic.topic,
            partitions: topic.partitions
          }
        end)
    })
  end
end
