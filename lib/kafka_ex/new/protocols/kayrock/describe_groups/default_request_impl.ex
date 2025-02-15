defimpl KafkaEx.New.Protocols.Kayrock.DescribeGroups.Request,
  for: [Kayrock.DescribeGroups.V0.Request, Kayrock.DescribeGroups.V1.Request] do
  def build_request(request_template, opts) do
    group_ids = Keyword.fetch!(opts, :group_names)
    Map.put(request_template, :group_ids, group_ids)
  end
end
