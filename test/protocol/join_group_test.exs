defmodule  KafkaEx.Protocol.JoinGroup.Test do
  use ExUnit.Case, async: true
  alias KafkaEx.Protocol.JoinGroup

  test "create_request creates a valid join group request" do
    good_request = <<
        11 :: 16, 0 :: 16, 42 :: 32, 9 :: 16, "client_id" :: binary, # Preamble
         5 :: 16, "group" :: binary, # GroupId
      3600 :: 32, # SessionTimeout
         9 :: 16, "member_id" :: binary, # MemberId
         8 :: 16, "consumer" :: binary, # ProtocolType
         1 :: 32, # GroupProtocols array size
         5 :: 16, "range" :: binary, # Basic strategy, "roundrobin" has some restrictions
         32 :: 32, # ProtocolMetadata length
         0 :: 16, # v0
         2 :: 32, 9 :: 16, "topic_one" :: binary, 9 :: 16, "topic_two" :: binary, # Topics array
         0 :: 32 >> # UserData
    request = JoinGroup.create_request(
      %JoinGroup.Request{
        correlation_id: 42,
        client_id: "client_id",
        member_id: "member_id",
        group_name: "group",
        topics: ["topic_one", "topic_two"],
        session_timeout: 3600
      }
    )
    assert request == good_request
  end

  test "parse success response correctly" do
    response = <<
        42 :: 32, # CorrelationId
        0 :: 16, # ErrorCode
        123 :: 32, # GenerationId
        8 :: 16, "consumer" :: binary, # GroupProtocol
        10 :: 16, "member_xxx" :: binary, # LeaderId
        10 :: 16, "member_one" :: binary, # MemberId
        2 :: 32, 10 :: 16, "member_one", 10 :: 16, "member_two" # Members array
      >>
    expected_response = %JoinGroup.Response{error_code: :no_error,
      generation_id: 123, leader_id: "member_xxx",
      member_id: "member_one", members: ["member_two", "member_one"]}
    assert JoinGroup.parse_response(response) == expected_response
  end
end
