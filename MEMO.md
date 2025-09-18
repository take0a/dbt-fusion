# dbt-fusion を読む

## 前提
- 2025/9/16 時点のソースコードの [fork](https://github.com/take0a/dbt-fusion)
- Commit 6832531（2025/9/14）まで

## 準備
- [The Rust Programming Language 日本語版](https://doc.rust-jp.rs/book-ja/)
- [プログラミングRust 第2版](https://www.oreilly.co.jp/books/9784873119786/)

## メモ
### README.md
- Top Level Components Released to Date
    - `dbt-jinja` - All Rust extension of mini-jinja to support dbt's jinja functions & other capabilities
    - `dbt-parser` - Rust parser for dbt projects
    - `dbt-snowflake` - database driver
    - `dbt-schemas` - complete, correct, machine generated json schemas for dbt's authoring surface
- Compiling from Source
    - The primary CLI in this repository is the `dbt-sa-cli`. 
- License
    - The dbt Fusion engine is a monorepo and contains more than one License.

### Cargo.toml
- workspace member
    - dbt-test-primitives
    - dbt-agate
    - dbt-auth
    - dbt-cancel
    - dbt-common
    - dbt-cloud-api
    - dbt-init
    - dbt-error
    - dbt-frontend-common
    - dbt-jinja/minijinja
    - minijinja-typecheck-builtins
    - dbt-jinja/minijinja-contrib
    - dbt-dag
    - dbt-loader
    - dbt-test-containers
    - dbt-parser
    - dbt-schemas
    - dbt-telemetry
    - dbt-selector-parser
    - dbt-proc-macros

    - dbt-init
    - dbt-adapter-proc-macros
    - dbt-fusion-adapter
    - dbt-ident
    - dbt-jinja-utils
    - dbt-sa-cli
    - dbt-xdbc
    - dbt-deps
    - vortex-client
    - vortex-events
    - proto-rust
    - dbt-env
    - dbt-test-utils

- workspace.package
    - authors = ["dbt Labs <info@getdbt.com>"]
    - description = "Fusion: A fast dbt engine, SQL compiler, local development framework, and in-memory analytical database"
    - edition = "2024"
    - homepage = "https://getdbt.com"
    - keywords = ["sql", "parquet", "json", "csv", "dbt"]
    - license = "Elastic-2.0"
    - repository = "https://github.com/dbt-labs/dbt-fusion"
    - version = "2.0.0-preview.14"

- workspace.dependencies のうち、member でないもの
    - https://github.com/sdf-labs/
        - antlr4
        - arrow-rs
        - dbt-serde-yaml
    - https://github.com/dbt-labs
        - arrow-adbc
    - ＃ serde
        - serde
        - serde_derive
        - serde_json
        - serde_repr
        - serde_with
    - toml
    - ＃ cloud providers
        - aws-config
        - aws-sdk-s3
    - ＃ arrow + parquet
        - arrow
        - arrow-array
        - arrow-buffer
        - arrow-csv
        - arrow-data
        - arrow-ipc
        - arrow-json
        - arrow-schema
        - parquet
        - serde_arrow
    - ＃ datafusion
        - 注: `datafusion` (別名 `datafusion-core`) クレートへの依存を抑制するため、以下の行は削除されました。`datafusion` クレートは、より細分化された他の datafusion-xxx クレートからシンボルをプルして再エクスポートし、datafusion 実行ランタイム用の API を追加する集約クレートです。ただし、個々のクレートよりもビルド速度が大幅に低下します。
        したがって、`datafusion` クレートに依存するのは、厳選された少数のクレート (具体的には、datafusion ランタイムとやり取りする必要があるクレート) のみにしてください。その他のクレートについては、可能な限り個々の datafusion-xxx クレートを直接使用することをお勧めします。
        - datafusion
        - datafusion-catalog
        - datafusion-common
        - datafusion-expr
        - datafusion-functions
            - datafusion-functions-aggregate
            - datafusion-functions-nested
            - datafusion-functions-window
        - datafusion-physical-expr
        - datafusion-physical-plan
    - uuid
    - ＃ networking & cache
        - redis
        - rustls
            - rustls-platform-verifier
        - reqwest
            - reqwest-middleware
            - reqwest-retry
        - gcloud-auth
        - http
        - token-source
        - ureq
    - lazy_static
    - ＃ type derived macros
        - enum-map
        - int-enum
        - strum
            - strum_macros
    - ＃ macros
        - paste
        - scopeguard
    - ＃ io stuff
        - console
        - csv
        - dialoguer
        - dirs
        - flate2
        - gix
        - gix-discover
        - glob
        - ignore
        - num_cpus
        - pathdiff
        - run_script
        - rust-embed
        - schemars：Generate JSON Schemas from Rust code
        - shellexpand
        - stringmetrics
        - tar
        - tempfile
        - term_size
        - walkdir
        - xshell
    - ＃ async + threading
        - async-trait
        - crossbeam-queue
        - crossbeam-skiplist
        - futures
            - futures-core
            - futures-lite
            - futures-util
        - once_cell
        - parking_lot
        - tokio
    - arc-swap
    - ＃ pretty
        - comfy-table
        - indicatif
    - rustyline
    - ＃ datatypes and algos
        - base64
        - bigdecimal
        - blake3
        - chrono
            - chrono-tz
        - clap : A simple to use, efficient, and full-featured Command Line Argument Parser
        - counter
        - dashmap
        - fancy-regex
        - hex
        - humantime
        - iana-time-zone
        - im
        - indexmap
        - ini
        - itertools
        - linked-hash-map
        - linked_hash_set
        - md5
        - petgraph
        - pkcs8
        - rand
        - regex
        - rsa
        - rust-lapper
        - sha2
        - similar
        - siphasher
        - url
        - ustr
        - zstd-safe
    - ＃ testing
        - difference
        - fake
        - goldie
        - indoc
        - merge
        - mock_instant
        - mockito
        - pretty_assertions
        - sqllogictest
    - ＃ protobuf
        - bytes
        - pbjson
            - pbjson-build
            - pbjson-types
        - prost
            - prost-build
            - prost-types
    - jsonschema-valid
    - ＃ phf
        - phf
            - phf_shared
    - ＃ error handling
        - anyhow
        - thiserror
    - ＃ database / orms
        - sea-orm
        - sqlx
    - ＃ logging
        - env_logger
        - log
    - ＃ tracing (observability)
        - tracing
            - tracing-log
            - tracing-subscriber
    - ＃ OpenTelemetry (optional embedded OTLP exporter for traces, see dbt-common)
        - opentelemetry
            - opentelemetry-otlp
            - opentelemetry-semantic-conventions
            - opentelemetry_sdk
    - tracy-client

### dbt-cancel
#### lib.rs
CancellationTokenSource と CancellationToken の実装
- CancellationToken は、キャンセルされたかどうかの状態を保持でき、確認できる。
- 内部状態は、Atomic を二重化して隠蔽しているので、スレッドセーフ。
- CancellationTokenSource は、Token を作る。
- Source が Drop したら Token はキャンセル。
- Golang の ctx のうち、キャンセル部分を自力で実装か？


### dbt-sa-cli
- たぶん、sa は、source available。sa でない dbt-cli も予定されているっぽい。
#### dbt_sa_clap.rs
clap（Command Line Argument Parser）のラッパー
- `#[derive(Parser)]` してるから `#[command]` できるみたい。
    - `#[...]` は、アウターアトリビュートで、直後の要素に適用される。
    - `#![...]` は、インナーアトリビュートで、モジュールやクレートに適用される。
    - https://doc.rust-jp.rs/rust-by-example-ja/attribute.html
- Cli 構造体にコマンドと引数を格納する
#### main.rs
- dbt_sa_clap と clap でコマンドと引数をパースして Cli 構造体に格納し、dbt_common::SystemArgs としても使う。
- dbt-common/tracing によるログ出力を初期化する
- vortex-events::fusion_sa_event_emitter を呼び出し、SA 用の _event_emitter を生成する（が生成するだけ）
- tokio ランタイムをスレッド数 1 または最大で生成する
- 実行時の panic をフックして、標準出力とエラー出力をフラッシュして、2 で返る
- dbt_sa_lib の execute_fs 非同期関数を呼び出して future を作る
- future の終了を待って、終了する
#### dbt_sa_lib.rs
- execute_fs
    - do_execute_fs
        - Man の場合は、dbt_schemas::execute_man_command へ
        - Init の場合は、dbt_init::iniit::run_init_workflow へ
        - その他の場合は、execute_setup_and_all_phases へ
- execute_setup_and_all_pjases
    - Clean の場合は、dbt_loader::clean::execute_clean_command へ
    - その他（Deps, Parse, List, Ls）の場合は、execute_all_phases へ
- execute_all_phases
    - dbt_loader::load でプロジェクトのファイルを読み込む
    - dbt_parser::resolver::resolve で sql を解析する
    - dbt_schemas::schemas::manifest::manifest::build_manifest で manifest を作り、書き出す


### dbt_common
#### tracing/*
- ログ出力の独自実装
#### tracing/README.md -> README-ja.md
#### discrete_event_emitter.rs
- DiscreteEventEmitter トレイトの定義
#### io_utils.rs
- try_read_yml_to_str
    - エラー処理付きで File::open して、file.read_to_string して返す。
#### node_selector.rs
- parse_model_specifiers
    - トークン化された CLI リスト（Clap によって空白で分割済み）を `SelectExpression` ツリーに変換します。



### vortex-events
#### event_functions.rs
- DiscreteEventEmitter のソース利用可能な実装。


### dbt_loader
#### loader.rs
- load
    - load_simplified_project_and_profiles で Project と Profile を取得する
        - dbt_jinja_utils::serde::value_from_file で DBT_PROJECT_YML を読み込む
        - jinja の環境を作り、dbt_jinja_utils::serde::into_typed_with_jinja で上の値に適用する
        - 必要なものが揃っているか確認してから load_profiles した結果とともに返す
    - dbt_profile.set_threads する
    - DbtState に DbtProfile をセットする（DbtProfile には DbtConfig が入っている）
    - ここでも jinja 環境を作る
    - prev_dbt_state があれば、上の jinja 環境とともに、load_inner する
        - 依存関係があれば、依存関係のあるパッケージも集める
        - load_project_yml で、プロジェクト要素のパスを補ったものを用意する
        - load_vars で、vars を設定する
        - 各種ファイルを集める
        - find_files_by_kind_and_extension で DbtAsset として集める
        - 各種 YAML は、DbtPackage.dbt_properties に集める
        - YAML 以外のファイルは、それぞれの files として、DbtPackage に収めて返す。
        - DbtPackage には、DbtProject も含まれる
        - この場合は、ここで load も返る
    - 以下は、prev_dbt_state がない場合
    - get_package_install_path して、persist_internal_packages する
    - fs_deps::get_or_install_packages でパッケージのロードとインストールをする（deps）
    - download_publication_artifacts で上流の公開アーティファクトを取得する
    - deps の場合は、ここまで。以下は、Parse か List。
    - 上記をそれぞれ load_packages、load_internal_packages して、DbtState の packages と vars に入れて返す
- load_profiles
    - profiles.yml などを探して
    - read_profiles_and_extract_db_config で DbConfig を作る
    - database, schema と合わせて、DbtProfile を作って返す
- find_files_by_kind_and_extension
    - 指定されたパスと拡張子でパスを集める
    - should_exclude_path で、TestPaths なら generic 除外する
    - `Vec<dbt_schemas::state::DbtAsset>` として返す
#### utils.rs
- read_profiles_and_extract_db_config
    - load と同様に value_from_file で読込む
    - load と同様に into_typed_with_jinja で jinja を適用して DbConfig を返す
#### dbt_project_yml_loader.rs
- load_project_yml
    - into_typed_with_jinja で読む（load_simplified_project_and_profiles と同様）
    - プロジェクト要素のパスのデフォルト値を設定する
#### load_vars.rs
- load_vars
    - vars_val があれば、vars を介して変数を集め、拡張して collected_vars で返す。
    - collected_vars があれば、拡張して返す
    - 両方ともなければ、空の変数を返す
#### load_packages.rs
- load_packages
    - 依存パッケージのパスを取集する
    - collect_packages
- load_internal_packages
    - internal_packages_dir を集める
    - collect_packages
- collect_packages
    - load_inner して package を DbtPackage にして返す
#### clean.rs
- execute_clean_command
    - path を集めて消す


### dbt_jinja_utils
#### serde.rs
- value_from_file
    - dbt_common::io_utils::try_read_yml_to_str して、value_from_str した結果の 
    `dbt_error::types::FsResult<dbt_serde_yaml::value::Value>` を返す
- value_from_str
    - dbt-serde-yaml パッケージの機能で YAML をデータとして読む
- into_typed_with_jinja
    - into_typed_with_jinja_error
        - into_typed_internal
            - dbt_serde_yaml::value::de::Value.into_typed に jinja 変換クロージャを渡して変換したものを返す
- from_yaml_raw
    - value_from_str して
    - into_typed_internal して返す
- into_typed_internal
    - dbt_serde_yaml::value::Value.into_typed を呼ぶ
#### phases/parse/resolve_context.rs
- build_resolve_context
    - モデルを解析するためのコンテキストを構築する
    - **いろいろ決め打ちで構築しているが詳細は保留**


### dbt_error
#### types.rs
- `pub type FsResult<T, E = Box<FsError>> = Result<T, E>`
- `pub struct FsError`
- なんで Fs？ File System？


### https://github.com/sdf-labs/dbt-serde-yaml
#### value/mod.rs
- `pub enum Value` YAML の構成要素の enum（カニ本の JSON の YAML 版）


### dbt_parser
#### resolver.rs
- resolve
    - 解析フェーズの入り口
    - データ構造は、dbt_schemas で定義されたもの
    - resolve_macros と resolve_docs_macros でマクロを解決する
    - resolve_operations で dbt_schemas::state::Operations の on_run_start と on_run_end を解決する
    - dbt_common::adapter::AdapterType を判定して、resolve_package_quoting する
    - dbt_jinja_utils::phases::parse::init::initialize_parse_jinja_environment で jinja 環境を作成する
    - resolve_final_selectors で selectors.yaml と指定されたセレクターから最終的なセレクターを計算する
    - resolve_packages_sequentially または resolve_packages_parallel で処理する
    - check_relation_uniqueness などチェックをして返す
- resolve_packages_sequentially
    - package ごとに resolve_package を呼ぶ
- resolve_packages_parallel
    - package ごとに resolve_package を並列に呼ぶ
- resolve_package
    - resolve_inner して返す
- resolve_inner
    - resolve_minimal_properties で schema.yml を解決する
    - 上で解決した minimal_properties に dbt_jinja_utils::serde::into_typed_with_jinja を適用する
    - resolve_sources で sources を解決して拡張する
    - resolve_seeds で seeds を解決して拡張する
    - resolve_snapshots で snapshots を解決して拡張する
    - resolve_models で models を解決して拡張する
    - resolve_analyses で analyses を解決して拡張する
    - resolve_exposures で exposures を解決して拡張する
    - resolve_semantic_models で semantic_models を解決して拡張する
    - resolve_metrics は途中かな？
    - resolve_saved_queries で saved_queries を解決して拡張する
    - resolve_data_tests で data_tests を解決して拡張する
    - resolve_unit_tests で unit_tests を解決して拡張する
    - resolve_groups で groups を解決して拡張する
#### resolve/resolve_macro.rs
- resolve_docs_macros
    - 引数の docs_macro_files ごとに、ファイルを読んで、parse_macro_statements して、マップに集めて返す
- resolve_macros
    - 引数の macro_files ごとに、ファイルを読んで、parse_macro_statements して、マップに集めて返す
    - **ここは結構頑張っているが、マクロの詳細が必要になるまで保留**
#### utils.rs
- parse_macro_statement
    - minijinja::compiler::parser::Parser を作り、parse_top_level_statements する
    - extract_sql_resource_from_ast して返す
- extract_sql_resources_from_ast
    - 引数の minijinja::compiler::ast::Stmt が Macro の場合
        - 引数の `Vec<dbt_jinja_utils::phases::parse::sql_resource::SqlResource>` に追加する
        - extract_sql_resource_from_ast を再帰呼び出しする
    - Template の場合も extract_sql_resources_from_ast を再帰呼び出しする
    - EmitRaw の場合は、引数の last_func_sign を設定する
#### resolve/resolve_operations.rs
- resolve_operations
    - DbtProject の on_run_start と on_run_end から必要なだけ new_opration して返す
    - **on_run_start と on_run_end は、JSON スキーマで定義されている**
- new_operation
    - DbtOperation に name と id を付与する
#### resolve/resolve_selectors.rs
- resolve_final_selectors
    - selectors.yaml が無ければ、CLI の引数から求める
    - 存在していれば、dbt_jinja_utils::serde::value_from_file でデータに読み込む
    - dbt_jinja_utils::parses::parse::resolve_context::build_resolve_context して
    - dbt_jinja_utils::serde::into_typed_with_jinja で dbt_schemas::schema::selectors::SelectorFile にする
    - dbt_selector_parser::parser::SelectorParser::parse_definition してマップを作る
    - デフォルトのセレクタを見つけておく
    - 明示的なセレクタかデフォルトかCLI引数からセレクタを解決して戻す
#### resolve/resolve_properties.rs
- resolve_minimal_properties
    - dbt_jinja_utils::serde::from_yaml_raw の戻りを other として、MinimalProperties.extend_from_minimal_properties_file を呼び出す
- MinimalProperties.extend_from_minimal_properties_file
    - other.models を拡張する
    - other.souces の source.tables の table を拡張する
    - other.seeds を拡張する
    - other.snapshots を拡張する
    - other.exposures を拡張する
    - other.saved_queries を拡張する
    - other.unit_tests を拡張する
    - other.tests を拡張する
    - other.data_tests を拡張する
    - other.groups を拡張する
#### resolve/resolve_sources.rs
- resolve_sources
#### resolve/resolve_seeds.rs
- resolve_seeds
#### resolve/resolve_snapshots.rs
- resolve_snapshots
#### resolve/resolve_models.rs
- resolve_models
#### resolve/resolve_analyses.rs
- resolve_analyses
#### resolve/resolve_exposures.rs
- resolve_exposures
#### resolve/resolve_semantic_models.rs
- resolve_semantic_models
#### resolve/resolve_metrics.rs
- resolve_metrics
    - 9/16 現在は、空が返る
#### resolve/resolve_saved_queries.rs
- resolve_saved_queries
#### resolve/resolve_data_tests.rs
- resolve_data_tests
#### resolve/resolve_unit_tests.rs
- resolve_unit_tests
#### resolve/resolve_groups.rs
- resolve_groups


### dbt_schemas
#### dbt_utils.rs
- resolve_package_quoting
    - DbtQuote の入力があれば、そこから作るか Snowflake に振る。
    - なければ、adapter_type が Snowflake かどうかで判定する。
#### main.rs
- execute_man_command
    - schemars の機能で自動生成したものを使っている？
    - schema が Profile の場合、SchemaGenerator の into_root_schema_for を DbtProfile 型で呼び出す
    - Project の場合、DbtProject 型で呼び出す
    - Selector の場合、SelectorFile 型で呼び出す
    - Schema の場合、DbtPropertiesFile 型で呼び出す
    - Telemetry の場合、TelemetryRecord 型で呼び出す
#### schemas/manifest//manifest.rs
- build_manifest


### dbt_selector_parser
#### parser.rs
- SelectorParser.parse_definition
    - 引数が文字列なら、dbt_common::node_selector::parse_model_secifiers へ
    - 引数が式なら、self.parse_expr へ
- SelectorParser.parse_expr
    - Composite 式なら self.parse_composite へ
    - Atom なら selft.parse_atom へ
- SelectorParser.parse_composite
    - 式と値から包含式が除外式に解析する
- SelectorParser.parse_atom
    - Method の場合、
        - selector なら self.parse_named へ。
        - selector でないなら self.atom_to_select_expression へ
    - MethodKey の場合は、self.atom_to_select_expression へ
- SelectorParser.parse_named
    - self.parse_definition へ再帰する
- SelectorParser.atom_to_select_expression
    - Method の場合、method と args を解決して、ネストしていたら再帰して解決して、SelectExpression::Atom を返す
    - MethodKey の場合、SelectExpression::Atom を返す
    - Exclude の場合、SelectExpression::Exclude を返す


### dbt_init
#### init.rs
- run_init_workflow
    - init_project
    - create_or_update_vscode_extensions
    - ProfileSetup.setup_profile
#### profile_setup.rs
- ProfileSetup.setup_profile
    - self.create_profile_for_adapter
    - self.write_profile
- ProfileSetup.create_profile_for_adapter
    - アダプタ型の dbt_schemas::schemas::profiles::DbConfig を作る
    - Snowflake の場合、dbt_init::adapter_config::snowflake_config::setup_snowflake_profile
    - BigQuery の場合、dbt_init::adapter_config::bigquery_config::setup_bigquery_profile
    - Databricks の場合、dbt_init::adapter_config::databricks_config::setup_databricks_profile
    - Postgres の場合、dbt_init::adapter_config::postgres_config::setup_postgres_profile
    - Redshift の場合、dbt_init::adapter_config::redshift_config::setup_redshift_profile
- ProfileSetup.write_profile
    - 既存のコンテンツ、順序、コメントを保持しながら、
      適切な profiles.yml に単一のプロファイル ブロックを記述または更新します。
#### adpter_config/snowflake_config.rs
- setup_snowflake_profile
#### adpter_config/bigquery_config.rs
- setup_bigquery_profile
#### adpter_config/databricks_config.rs
- setup_databricks_profile
#### adpter_config/postgres_config.rs
- setup_postgres_profile
#### adpter_config/
- setup_redshift_profile


### dbt-deps = fs_deps
#### mod.rs
- get_or_install_packages


