defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch.Response, as: OffsetFetchResponse

  alias Kayrock.OffsetFetch.V0
  alias Kayrock.OffsetFetch.V1
  alias Kayrock.OffsetFetch.V2
  alias Kayrock.OffsetFetch.V3

  @consumer_group "test-group"
  @valid_response %KafkaEx.New.Structs.OffsetFetch{
    consumer_group: "test-group",
    topics: [
      %KafkaEx.New.Structs.OffsetFetch.TopicOffsets{
        topic: "test-topic",
        committed_offsets: [
          %KafkaEx.New.Structs.OffsetFetch.CommittedOffset{
            partition: 1,
            offset: 100,
            metadata: "metadata",
            error_code: 0
          }
        ]
      }
    ]
  }

  describe "parse_response/1" do
    test "for v0 - it parses offset fetch response" do
      response = %V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partitions: [
              %{partition: 1, offset: 100, metadata: "metadata", error_code: 0}
            ]
          }
        ]
      }

      {:ok, offsets} = OffsetFetchResponse.parse_response(response, @consumer_group)

      assert offsets == @valid_response
    end

    test "for v1 - it parses offset fetch response" do
      response = %V1.Response{
        responses: [
          %{
            topic: "test-topic",
            partitions: [
              %{partition: 1, offset: 100, metadata: "metadata", error_code: 0}
            ]
          }
        ]
      }

      {:ok, offsets} = OffsetFetchResponse.parse_response(response, @consumer_group)

      assert offsets == @valid_response
    end

    test "for v2 - it parses offset fetch response" do
      response = %V2.Response{
        responses: [
          %{
            topic: "test-topic",
            partitions: [
              %{partition: 1, offset: 100, metadata: "metadata", error_code: 0}
            ]
          }
        ]
      }

      {:ok, offsets} = OffsetFetchResponse.parse_response(response, @consumer_group)

      assert offsets == @valid_response
    end

    test "for v2 - it returns error when offset fetch failed" do
      response = %V2.Response{
        responses: [
          %{
            topic: "test-topic",
            partitions: [
              %{partition: 1, offset: 100, metadata: "metadata", error_code: 1}
            ]
          }
        ],
        error_code: 1
      }

      {:error, :offset_out_of_range} = OffsetFetchResponse.parse_response(response, @consumer_group)
    end

    test "for v3 - it parses offset fetch response" do
      response = %V3.Response{
        responses: [
          %{
            topic: "test-topic",
            partitions: [
              %{partition: 1, offset: 100, metadata: "metadata", error_code: 0}
            ]
          }
        ]
      }

      {:ok, offsets} = OffsetFetchResponse.parse_response(response, @consumer_group)

      assert offsets == @valid_response
    end

    test "for v3 - it returns error when offset fetch failed" do
      response = %V3.Response{
        responses: [
          %{
            topic: "test-topic",
            partitions: [
              %{partition: 1, offset: 100, metadata: "metadata", error_code: 1}
            ]
          }
        ],
        error_code: 1
      }

      {:error, :offset_out_of_range} = OffsetFetchResponse.parse_response(response, @consumer_group)
    end
  end
end
