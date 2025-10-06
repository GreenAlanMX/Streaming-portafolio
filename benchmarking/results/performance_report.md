
# ETL Pipeline Performance Benchmark Report

## Executive Summary
- Total benchmark runs: 5
- Successful runs: 0
- Failed runs: 5

## Performance Results

### SMALL Dataset
- **Status**: ❌ FAILED
- **Records**: 6,100
- **Execution Time**: 8.42 seconds
- **Throughput**: 724.33 records/second
- **Memory Peak**: -148.64 MB
- **Error**: /Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:24:50.134 | INFO    | prefect - Starting temporary server on http://127.0.0.1:8918
See https://docs.prefect.io/v3/concepts/server#how-to-guides for more information on running a dedicated Prefect server.
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:24:53.042 | INFO    | Flow run 'delectable-quoll' - Beginning flow run 'delectable-quoll' for flow 'etl-pipeline'
15:24:53.068 | WARNING | root - Se corrigieron 100 valores inválidos en release_year
15:24:53.091 | ERROR   | Flow run 'delectable-quoll' - Encountered exception during execution: KeyError("Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist")
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:24:53.101 | INFO    | Flow run 'delectable-quoll' - Finished in state Failed('Flow run encountered an exception: KeyError: "Column(s) [\'completion_percentage\', \'watch_duration_minutes\'] do not exist"')
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 438, in <module>
    etl_pipeline("real")
    ~~~~~~~~~~~~^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flows.py", line 1702, in __call__
    return run_flow(
        flow=self,
    ...<2 lines>...
        return_type=return_type,
    )
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1552, in run_flow
    ret_val = run_flow_sync(**kwargs)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1397, in run_flow_sync
    return engine.state if return_type == "state" else engine.result()
                                                       ~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 361, in result
    raise self._raised
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:24:53.108 | INFO    | prefect - Stopping temporary server on http://127.0.0.1:8918


### MEDIUM Dataset
- **Status**: ❌ FAILED
- **Records**: 30,500
- **Execution Time**: 8.44 seconds
- **Throughput**: 3614.72 records/second
- **Memory Peak**: -23.94 MB
- **Error**: /Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:24:58.985 | INFO    | prefect - Starting temporary server on http://127.0.0.1:8872
See https://docs.prefect.io/v3/concepts/server#how-to-guides for more information on running a dedicated Prefect server.
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:25:01.943 | INFO    | Flow run 'prudent-swan' - Beginning flow run 'prudent-swan' for flow 'etl-pipeline'
15:25:01.980 | WARNING | root - Se corrigieron 100 valores inválidos en release_year
15:25:02.020 | ERROR   | Flow run 'prudent-swan' - Encountered exception during execution: KeyError("Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist")
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:25:02.035 | INFO    | Flow run 'prudent-swan' - Finished in state Failed('Flow run encountered an exception: KeyError: "Column(s) [\'completion_percentage\', \'watch_duration_minutes\'] do not exist"')
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 438, in <module>
    etl_pipeline("real")
    ~~~~~~~~~~~~^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flows.py", line 1702, in __call__
    return run_flow(
        flow=self,
    ...<2 lines>...
        return_type=return_type,
    )
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1552, in run_flow
    ret_val = run_flow_sync(**kwargs)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1397, in run_flow_sync
    return engine.state if return_type == "state" else engine.result()
                                                       ~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 361, in result
    raise self._raised
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:25:02.045 | INFO    | prefect - Stopping temporary server on http://127.0.0.1:8872


### LARGE Dataset
- **Status**: ❌ FAILED
- **Records**: 61,000
- **Execution Time**: 7.83 seconds
- **Throughput**: 7792.14 records/second
- **Memory Peak**: 0.23 MB
- **Error**: /Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:25:08.564 | INFO    | prefect - Starting temporary server on http://127.0.0.1:8723
See https://docs.prefect.io/v3/concepts/server#how-to-guides for more information on running a dedicated Prefect server.
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:25:11.252 | INFO    | Flow run 'calm-gorilla' - Beginning flow run 'calm-gorilla' for flow 'etl-pipeline'
15:25:11.295 | WARNING | root - Se corrigieron 100 valores inválidos en release_year
15:25:11.345 | ERROR   | Flow run 'calm-gorilla' - Encountered exception during execution: KeyError("Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist")
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:25:11.355 | INFO    | Flow run 'calm-gorilla' - Finished in state Failed('Flow run encountered an exception: KeyError: "Column(s) [\'completion_percentage\', \'watch_duration_minutes\'] do not exist"')
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 438, in <module>
    etl_pipeline("real")
    ~~~~~~~~~~~~^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flows.py", line 1702, in __call__
    return run_flow(
        flow=self,
    ...<2 lines>...
        return_type=return_type,
    )
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1552, in run_flow
    ret_val = run_flow_sync(**kwargs)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1397, in run_flow_sync
    return engine.state if return_type == "state" else engine.result()
                                                       ~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 361, in result
    raise self._raised
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:25:11.364 | INFO    | prefect - Stopping temporary server on http://127.0.0.1:8723


### XLARGE Dataset
- **Status**: ❌ FAILED
- **Records**: 122,000
- **Execution Time**: 7.55 seconds
- **Throughput**: 16164.13 records/second
- **Memory Peak**: -15.00 MB
- **Error**: /Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:25:17.991 | INFO    | prefect - Starting temporary server on http://127.0.0.1:8744
See https://docs.prefect.io/v3/concepts/server#how-to-guides for more information on running a dedicated Prefect server.
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:25:20.789 | INFO    | Flow run 'vigilant-gecko' - Beginning flow run 'vigilant-gecko' for flow 'etl-pipeline'
15:25:20.861 | WARNING | root - Se corrigieron 100 valores inválidos en release_year
15:25:20.939 | ERROR   | Flow run 'vigilant-gecko' - Encountered exception during execution: KeyError("Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist")
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:25:20.948 | INFO    | Flow run 'vigilant-gecko' - Finished in state Failed('Flow run encountered an exception: KeyError: "Column(s) [\'completion_percentage\', \'watch_duration_minutes\'] do not exist"')
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 438, in <module>
    etl_pipeline("real")
    ~~~~~~~~~~~~^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flows.py", line 1702, in __call__
    return run_flow(
        flow=self,
    ...<2 lines>...
        return_type=return_type,
    )
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1552, in run_flow
    ret_val = run_flow_sync(**kwargs)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1397, in run_flow_sync
    return engine.state if return_type == "state" else engine.result()
                                                       ~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 361, in result
    raise self._raised
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:25:20.955 | INFO    | prefect - Stopping temporary server on http://127.0.0.1:8744


### XXLARGE Dataset
- **Status**: ❌ FAILED
- **Records**: 305,000
- **Execution Time**: 8.01 seconds
- **Throughput**: 38087.44 records/second
- **Memory Peak**: 0.12 MB
- **Error**: /Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:25:29.117 | INFO    | prefect - Starting temporary server on http://127.0.0.1:8297
See https://docs.prefect.io/v3/concepts/server#how-to-guides for more information on running a dedicated Prefect server.
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `pyproject_toml_table_header` is set in model_config but will be ignored because no PyprojectTomlConfigSettingsSource source is configured. To use this config key, add a PyprojectTomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pydantic_settings/main.py:425: UserWarning: Config key `toml_file` is set in model_config but will be ignored because no TomlConfigSettingsSource source is configured. To use this config key, add a TomlConfigSettingsSource source to the settings sources via the settings_customise_sources hook.
  self._settings_warn_unused_config_keys(sources, self.model_config)
15:25:32.023 | INFO    | Flow run 'unnatural-koel' - Beginning flow run 'unnatural-koel' for flow 'etl-pipeline'
15:25:32.184 | WARNING | root - Se corrigieron 100 valores inválidos en release_year
15:25:32.375 | ERROR   | Flow run 'unnatural-koel' - Encountered exception during execution: KeyError("Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist")
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:25:32.387 | INFO    | Flow run 'unnatural-koel' - Finished in state Failed('Flow run encountered an exception: KeyError: "Column(s) [\'completion_percentage\', \'watch_duration_minutes\'] do not exist"')
Traceback (most recent call last):
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 438, in <module>
    etl_pipeline("real")
    ~~~~~~~~~~~~^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flows.py", line 1702, in __call__
    return run_flow(
        flow=self,
    ...<2 lines>...
        return_type=return_type,
    )
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1552, in run_flow
    ret_val = run_flow_sync(**kwargs)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1397, in run_flow_sync
    return engine.state if return_type == "state" else engine.result()
                                                       ~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 361, in result
    raise self._raised
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 781, in run_context
    yield self
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 1395, in run_flow_sync
    engine.call_flow_fn()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/flow_engine.py", line 801, in call_flow_fn
    result = call_with_parameters(self.flow.fn, self.parameters)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/prefect/utilities/callables.py", line 210, in call_with_parameters
    return fn(*args, **kwargs)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 423, in etl_pipeline
    user_agg = aggregate_user_metrics(df)
  File "/Users/janeth/Streaming_Portfolio/etl/etl_pipeline_enhanced.py", line 316, in aggregate_user_metrics
    user_agg = grouped.agg(
               ~~~~~~~~~~~^
        sessions_count=("session_id", "count"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<7 lines>...
        country=("country", "first"),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).reset_index().fillna(0)
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
           ~~~~~~~~~~~~~~~~~~^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 1603, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
                                ~~~~~~~~~~~~~~~~~~~~~~^
        op_name, selected_obj, selection, kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/Users/janeth/Streaming_Portfolio/.venv/lib/python3.13/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['completion_percentage', 'watch_duration_minutes'] do not exist"
15:25:32.395 | INFO    | prefect - Stopping temporary server on http://127.0.0.1:8297

