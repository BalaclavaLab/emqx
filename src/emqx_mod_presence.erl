%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_mod_presence).

-behaviour(emqx_gen_mod).

-include("emqx.hrl").
-include("logger.hrl").
-include("emqx_gpb.hrl").

-logger_header("[Presence]").

%% emqx_gen_mod callbacks
-export([load/1
    , unload/1
    , description/0
]).

-export([on_client_connected/3
    , on_client_disconnected/4
]).

-ifdef(TEST).
-export([reason/1]).
-endif.

load(Env) ->
    {ok, _} = application:ensure_all_started(brod),
    KafkaBootstrapEndpointsString = proplists:get_value(kafka_bootstrap_endpoints, Env, "localhost:9092"),
    KafkaBootstrapEndpoints = parse_kafka_bootstrap_endpoints_string(KafkaBootstrapEndpointsString),
    PresenceTopic = list_to_binary(proplists:get_value(kafka_presence_topic, Env, "mqtt-presence-raw")),
    ClientConfig = [{reconnect_cool_down_seconds, 10}],
    ok = brod:start_client(KafkaBootstrapEndpoints, kafka_client, ClientConfig),
    ok = brod:start_producer(kafka_client, PresenceTopic, _ProducerConfig = []),
    emqx_hooks:add('client.connected', {?MODULE, on_client_connected, [Env]}),
    emqx_hooks:add('client.disconnected', {?MODULE, on_client_disconnected, [Env]}).

unload(_Env) ->
    emqx_hooks:del('client.connected', {?MODULE, on_client_connected}),
    emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}).

description() ->
    "EMQ X Presence Module".
%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

on_client_connected(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost,
        sockport := SockPort},
    _ConnInfo = #{
        clean_start := CleanStart,
        proto_name := ProtoName,
        proto_ver := ProtoVer,
        peername := {IpAddr, _},
        keepalive := Keepalive,
        connected_at := ConnectedAt,
        expiry_interval := ExpiryInterval
    }, Env) ->
    Milliseconds = erlang:system_time(millisecond),
    Presence = #{clientid => ClientId,
        username => Username,
        ipaddress => ntoa(PeerHost),
        sockport => SockPort,
        proto_name => ProtoName,
        proto_ver => ProtoVer,
        keepalive => Keepalive,
        connack => 0, %% Deprecated?
        clean_start => CleanStart,
        expiry_interval => ExpiryInterval,
        connected_at => ConnectedAt,
        ts => Milliseconds
    },
    case emqx_json:safe_encode(Presence) of
        {ok, Payload} ->
            _ = emqx_broker:safe_publish(
                make_msg(qos(Env), topic(connected, ClientId), Payload)),
            KafkaMessage = #'EmqxPresence'{
                username = Username,
                client_id = ClientId,
                time = Milliseconds,
                presence = {connected_message, #'ConnectedMessage'{
                    ip_address = ntoa(IpAddr),
                    conn_ack = emqx_gpb:'enum_symbol_by_value_ConnectedMessage.ConnAck'(0),
                    session = CleanStart,
                    protocol_version = ProtoVer}
                }},
            PresenceTopic = list_to_binary(proplists:get_value(kafka_presence_topic, Env, "mqtt-presence-raw")),
            PresenceTopicPartition = rand:uniform(proplists:get_value(kafka_presence_topic_partition_count, Env, 1)) - 1,
            ok = brod:produce_sync(kafka_client, PresenceTopic, PresenceTopicPartition, ClientId, emqx_gpb:encode_msg(KafkaMessage));
        {error, _Reason} ->
            ?LOG(error, "Failed to encode 'connected' presence: ~p", [Presence])
    end.

on_client_disconnected(_ClientInfo = #{clientid := ClientId, username := Username},
    Reason, _ConnInfo = #{disconnected_at := DisconnectedAt}, Env) ->
    Milliseconds = erlang:system_time(millisecond),
    Presence = #{clientid => ClientId,
        username => Username,
        reason => reason(Reason),
        disconnected_at => DisconnectedAt,
        ts => Milliseconds
    },
    case emqx_json:safe_encode(Presence) of
        {ok, Payload} ->
            emqx_broker:safe_publish(
                make_msg(qos(Env), topic(disconnected, ClientId), Payload)),
            KafkaMessage = #'EmqxPresence'{
                username = Username,
                client_id = ClientId,
                time = Milliseconds,
                presence = {disconnected_message, #'DisconnectedMessage'{
                    reason = reason_binary(Reason)
                }
                }},
            PresenceTopic = list_to_binary(proplists:get_value(kafka_presence_topic, Env, "mqtt-presence-raw")),
            PresenceTopicPartition = rand:uniform(proplists:get_value(kafka_presence_topic_partition_count, Env, 1)) - 1,
            ok = brod:produce_sync(kafka_client, PresenceTopic, PresenceTopicPartition, ClientId, emqttd_gpb:encode_msg(KafkaMessage));
        {error, _Reason} ->
            ?LOG(error, "Failed to encode 'disconnected' presence: ~p", [Presence])
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

make_msg(QoS, Topic, Payload) ->
    emqx_message:set_flag(
        sys, emqx_message:make(
            ?MODULE, QoS, Topic, iolist_to_binary(Payload))).

topic(connected, ClientId) ->
    emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/connected"]));
topic(disconnected, ClientId) ->
    emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/disconnected"])).

qos(Env) -> proplists:get_value(qos, Env, 0).

-compile({inline, [reason/1]}).
reason(Reason) when is_atom(Reason) -> Reason;
reason({shutdown, Reason}) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

reason_binary(Reason) when is_atom(Reason) -> atom_to_binary(Reason, latin1);
reason_binary({Error, _}) when is_atom(Error) -> <<"Error">>;
reason_binary(_) -> <<"internal_error">>.

parse_kafka_bootstrap_endpoints_string(KafkaBootstrapEndpointsString) ->
    lists:map(fun(HostPort) ->
        [Host, Port] = re:split(HostPort, "\:", [{return, list}]),
        {Host, list_to_integer(Port)} end,
        re:split(KafkaBootstrapEndpointsString, "\,", [{return, list}])).

-compile({inline, [ntoa/1]}).
ntoa(IpAddr) -> iolist_to_binary(inet:ntoa(IpAddr)).

