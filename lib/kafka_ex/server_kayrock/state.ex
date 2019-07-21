defmodule KafkaEx.ServerKayrock.State do
  @moduledoc false

  # state struct for ServerKayrock

  alias KafkaEx.New.ClusterMetadata

  defstruct(
    cluster_metadata: %ClusterMetadata{},
    correlation_id: 0,
    consumer_group_for_auto_commit: nil,
    metadata_update_interval: nil,
    consumer_group_update_interval: nil,
    worker_name: KafkaEx.Server,
    ssl_options: [],
    use_ssl: false,
    api_versions: %{},
    allow_auto_topic_creation: true
  )

  @type t :: %__MODULE__{}

  @spec increment_correlation_id(t) :: t
  def increment_correlation_id(%__MODULE__{correlation_id: cid} = state) do
    %{state | correlation_id: cid + 1}
  end

  def select_broker(
        %__MODULE__{cluster_metadata: cluster_metadata},
        selector
      ) do
    with {:ok, node_id} <-
           ClusterMetadata.select_node(cluster_metadata, selector),
         broker <- ClusterMetadata.broker_by_node_id(cluster_metadata, node_id) do
      {:ok, broker}
    else
      err -> err
    end
  end

  def update_brokers(
        %__MODULE__{cluster_metadata: cluster_metadata} = state,
        cb
      )
      when is_function(cb, 1) do
    %{
      state
      | cluster_metadata: ClusterMetadata.update_brokers(cluster_metadata, cb)
    }
  end

  def put_consumer_group_coordinator(
        %__MODULE__{cluster_metadata: cluster_metadata} = state,
        consumer_group,
        coordinator_node_id
      ) do
    %{
      state
      | cluster_metadata:
          ClusterMetadata.put_consumer_group_coordinator(
            cluster_metadata,
            consumer_group,
            coordinator_node_id
          )
    }
  end

  def remove_topics(
        %__MODULE__{cluster_metadata: cluster_metadata} = state,
        topics
      ) do
    %{
      state
      | cluster_metadata:
          ClusterMetadata.remove_topics(cluster_metadata, topics)
    }
  end

  def topics_metadata(
        %__MODULE__{cluster_metadata: cluster_metadata},
        wanted_topics
      ) do
    ClusterMetadata.topics_metadata(cluster_metadata, wanted_topics)
  end

  def brokers(%__MODULE__{cluster_metadata: cluster_metadata}) do
    ClusterMetadata.brokers(cluster_metadata)
  end
end