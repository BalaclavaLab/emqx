%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BRIDGE_CONF_DEFAULT, <<"bridges: {}">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

suite() ->
    [].

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    %% some testcases (may from other app) already get emqx_connector started
    _ = application:stop(emqx_resource),
    _ = application:stop(emqx_connector),
    ok = emqx_common_test_helpers:start_apps(
        [
            emqx_connector,
            emqx_bridge
        ]
    ),

    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([
        emqx_connector,
        emqx_bridge
    ]),
    ok.

init_per_testcase(_, Config) ->
    Config.
end_per_testcase(_, _Config) ->
    ok.

t_list_raw_empty(_) ->
    Result = emqx_connector:list_raw(),
    ?assertEqual([], Result).

t_lookup_raw_error(_) ->
    Result = emqx_connector:lookup_raw(<<"foo:bar">>),
    ?assertEqual({error, not_found}, Result).

t_parse_connector_id_error(_) ->
    ?assertError(
        {invalid_connector_id, <<"foobar">>}, emqx_connector:parse_connector_id(<<"foobar">>)
    ).

t_update_error(_) ->
    ?assertException(exit, {noproc, _}, emqx_connector:update(<<"foo:bar">>, #{})).

t_delete_error(_) ->
    ?assertException(exit, {noproc, _}, emqx_connector:delete(<<"foo:bar">>)).

t_connector_id_using_list(_) ->
    <<"foo:bar">> = emqx_connector:connector_id("foo", "bar").
