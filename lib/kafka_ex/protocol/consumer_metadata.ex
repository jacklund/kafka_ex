defmodule KafkaEx.Protocol.ConsumerMetadata do
  alias KafkaEx.Protocol

  defmodule Response do
    defstruct coordinator_id: 0, coordinator_host: "", coordinator_port: 0, error_code: 0
    @type t :: %Response{coordinator_id: integer, coordinator_host: binary, coordinator_port: 0..65535, error_code: integer}

    def broker_for_consumer_group(brokers, consumer_group_metadata) do
      Enum.find(brokers, &(&1.host == consumer_group_metadata.coordinator_host && &1.port == consumer_group_metadata.coordinator_port && &1.socket && is_list(Port.info(&1.socket))))
    end
  end

  @spec create_request(integer, binary, binary) :: <<_::16, _::_ *8>>
  def create_request(correlation_id, client_id, consumer_group) do
    KafkaEx.Protocol.create_request(:consumer_metadata, correlation_id, client_id) <> << byte_size(consumer_group) :: 16-signed, consumer_group :: binary >>
  end

  @spec parse_response(<<_::64, _::_ *8 >>) :: Response.t
  def parse_response(<< _corr_id :: 32-signed, error_code :: 16-signed, coord_id :: 32-signed, coord_host_size :: 16-signed, coord_host :: size(coord_host_size)-binary, coord_port :: 32-signed, _ :: binary >>) do
    %Response{coordinator_id: coord_id, coordinator_host: coord_host, coordinator_port: coord_port, error_code: Protocol.error(error_code)}
  end
end
