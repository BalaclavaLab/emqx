%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_presence).

-behaviour(emqx_gen_mod).

-include("emqx.hrl").
-include("logger.hrl").
-include("emqx_gpb.hrl").

-logger_header("[Presence]").

%% APIs
-export([on_client_connected/4
    , on_client_disconnected/3
]).

%% emqx_gen_mod callbacks
-export([load/1
    , unload/1
]).

-define(ATTR_KEYS, [clean_start, proto_ver, proto_name, keepalive]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

load(Env) ->
    {ok, _} = application:ensure_all_started(brod),
    KafkaBootstrapEndpointsString = proplists:get_value(kafka_bootstrap_endpoints, Env, "localhost:9092"),
    KafkaBootstrapEndpoints = parse_kafka_bootstrap_endpoints_string(KafkaBootstrapEndpointsString),
    PresenceTopic = list_to_binary(proplists:get_value(kafka_presence_topic, Env, "mqtt-presence-raw")),
    ClientConfig = [{reconnect_cool_down_seconds, 10}],
    ok = brod:start_client(KafkaBootstrapEndpoints, kafka_client, ClientConfig),
    ok = brod:start_producer(kafka_client, PresenceTopic, _ProducerConfig = []),
    emqx_hooks:add('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
    emqx_hooks:add('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]).

on_client_connected(
    #{client_id := ClientId, username   := Username, peername   := {IpAddr, _}},
    ConnAck,
    ConnAttrs = #{proto_ver := ProtoVer, clean_start := CleanStart},
    Env) ->
    Attrs = maps:filter(fun(K, _) ->
        lists:member(K, ?ATTR_KEYS)
                        end, ConnAttrs),
    Milliseconds = erlang:system_time(millisecond),
    case emqx_json:safe_encode(Attrs#{
        clientid => ClientId,
        username => Username,
        ipaddress => iolist_to_binary(esockd_net:ntoa(IpAddr)),
        connack => ConnAck,
        proto_ver => ProtoVer,
        ts => Milliseconds
    }) of
        {ok, Payload} ->
            Result = emqx:publish(message(qos(Env), topic(connected, ClientId), Payload)),
            KafkaMessage = #'EmqxPresence'{
                username = Username,
                client_id = ClientId,
                time = Milliseconds,
                presence = {connected_message, #'ConnectedMessage'{
                    ip_address = list_to_binary(emqttd_net:ntoa(IpAddr)),
                    conn_ack = emqx_gpb:'enum_symbol_by_value_ConnectedMessage.ConnAck'(ConnAck),
                    session = CleanStart,
                    protocol_version = ProtoVer}
                }},
            PresenceTopic = list_to_binary(proplists:get_value(kafka_presence_topic, Env, "mqtt-presence-raw")),
            PresenceTopicPartition = rand:uniform(proplists:get_value(kafka_presence_topic_partition_count, Env, 1)) - 1,
            ok = brod:produce_sync(kafka_client, PresenceTopic, PresenceTopicPartition, ClientId, emqx_gpb:encode_msg(KafkaMessage)),
            Result;
        {error, Reason} ->
            ?LOG(error, "Encoding connected event error: ~p", [Reason])
    end.

on_client_disconnected(#{client_id := ClientId, username := Username}, Reason, Env) ->
    Milliseconds = erlang:system_time(millisecond),
    case emqx_json:safe_encode([{clientid, ClientId},
        {username, Username},
        {reason, reason(Reason)},
        {ts, erlang:system_time(millisecond)}]) of
        {ok, Payload} ->
            emqx_broker:publish(message(qos(Env), topic(disconnected, ClientId), Payload)),
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
        {error, Reason} ->
            ?LOG(error, "Encoding disconnected event error: ~p", [Reason])
    end.

unload(_Env) ->
    emqx_hooks:del('client.connected', fun ?MODULE:on_client_connected/4),
    emqx_hooks:del('client.disconnected', fun ?MODULE:on_client_disconnected/3).

message(QoS, Topic, Payload) ->
    emqx_message:set_flag(
        sys, emqx_message:make(
            ?MODULE, QoS, Topic, iolist_to_binary(Payload))).

topic(connected, ClientId) ->
    emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/connected"]));
topic(disconnected, ClientId) ->
    emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/disconnected"])).

qos(Env) -> proplists:get_value(qos, Env, 0).

reason(Reason) when is_atom(Reason) -> Reason;
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
