defmodule KafkaEx.New.BrokerTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Broker

  setup do
    pid = Server.start(3040)

    {:ok, socket} =
      KafkaEx.Socket.create('localhost', 3040, [:binary, {:packet, 0}], false)

    on_exit(fn ->
      KafkaEx.Socket.close(socket)
      Process.exit(pid, :normal)
    end)

    {:ok, [socket: socket]}
  end

  describe "connect_broker/1" do
    @tag skip: true
    test "connects broker via socket" do
    end
  end

  describe "put_socket/2" do
    test "nullify broker socket" do
      broker = %Broker{socket: %KafkaEx.Socket{}} |> Broker.put_socket(nil)

      assert is_nil(broker.socket)
    end

    test "updates broker socket to new one" do
      socket = %KafkaEx.Socket{}
      broker = %Broker{socket: nil} |> Broker.put_socket(socket)

      assert broker.socket == socket
    end
  end

  describe "connected?/1" do
    test "returns false if socket is nil" do
      broker = %Broker{socket: nil}

      refute Broker.connected?(broker)
    end

    test "returns false if socket is not connected" do
      broker = %Broker{socket: nil}

      refute Broker.connected?(broker)
    end

    test "returns true if socket is connected", %{socket: socket} do
      broker = %Broker{socket: socket}

      assert Broker.connected?(broker)
    end
  end

  describe "has_socket?/1" do
    test "returns false if broker doesn't have a socket" do
      broker = %Broker{socket: nil}
      socket = %KafkaEx.Socket{}

      refute Broker.has_socket?(broker, socket)
    end

    test "returns false if broker has different socket", %{socket: socket_one} do
      {:ok, socket_two} =
        KafkaEx.Socket.create('localhost', 3040, [:binary, {:packet, 0}], false)

      broker = %Broker{socket: nil} |> Broker.put_socket(socket_one)

      refute Broker.has_socket?(broker, socket_two.socket)
      KafkaEx.Socket.close(socket_two)
    end

    test "returns true if broker has same socket", %{socket: socket} do
      broker = %Broker{socket: nil} |> Broker.put_socket(socket)

      assert Broker.has_socket?(broker, socket.socket)
    end
  end
end
