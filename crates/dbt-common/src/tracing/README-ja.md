# Tracing Infrastructure

このモジュールは、Fusion 向けの包括的なトレース インフラストラクチャを提供し、複数の目的に活用されます。
1. **統合スパン & イベント データ レイヤー** - システム内のすべての操作とイベントに関する唯一の信頼できる情報源
2. **構造化テレメトリ** - 下流システム（クラウド クライアント、オーケストレーション、メタデータなど）のアプリケーション パフォーマンス データとメトリクスをキャプチャ
3. **インタラクティブ ユーザー エクスペリエンス** - [TBD] CLI およびユーザー ログ用のデータをフォーマット
4. **開発者向けデバッグ** - リリースビルドにコンパイルされた豊富なデバッグ情報を提供

## Architecture Overview

トレース インフラストラクチャは、階層型アーキテクチャに従います。

```
┌─────────────────────────────────────────────────────────────────┐
│                    Log Facade                                   │
│                  (log crate API)                                │
│                 Legacy log! macros                              │
└─────────────────────────┬───────────────────────────────────────┘
                          │ (Bridge forwards to tracing)
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Tracing Facade                               │
│                (tracing crate API)                              │
│          tracing::instrument, tracing::info!, etc.              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                   Data Layer                                    │
│              (TelemetryDataLayer)                               │
│  - spans/events を構造化されたテレメトリレコードに変換します        │
│  - Writing Layers のためのスパン拡張にデータを保存します           │
│  - trace/span ID の生成と相関を処理                              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                 Writing Layers                                  │
│  ┌─────────────────────┐ ┌─────────────────────────────────┐    │
│  │ TelemetryWriterLayer│ │    OTLP Exporter Layer          │    │
│  │   (File output)     │ │ (OpenTelemetry Protocol)        │    │
│  │   - JSONL format    │ │   - Debug builds only           │    │
│  │   - Production use  │ │   - Feature gated               │    │
│  └─────────────────────┘ └─────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │          CLI & User Logs Layer                          │    │
│  │              [NOT IMPLEMENTED]                          │    │
│  │   - Pretty formatting for terminal output               │    │
│  │   - Progress bars and interactive elements              │    │
│  │   - User-facing log messages                            │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### Data Layer (`TelemetryDataLayer`)
- **目的**: トレーススパンとイベントを構造化されたテレメトリレコードに変換する
- **主な機能**:
    - プロセス全体でグローバルに一意のスパンIDを生成する
    - すべてのデータをトレースID（呼び出しUUIDから派生）と関連付ける
    - スパン/イベントフィールドから構造化属性を抽出する
    - コード位置の記録を処理する（リリースビルドでは削除される）
    - テレメトリデータをライターレイヤーのスパン拡張に保存する

### Writing Layers
- **File Writer** (`TelemetryWriterLayer`): 本番環境での使用のためにテレメトリをJSONLファイルに出力します
- **OTLP Exporter** (`OTLPExporterLayer`): OpenTelemetryプロトコルエンドポイントにエクスポートします (デバッグビルドのみ)

### Telemetry Records
すべてのテレメトリデータは、`dbt-telemetry/src/schemas/record.rs` で定義された構造化スキーマに従います。
- **SpanStartInfo**: スパンの開始時に出力されます。
- **SpanEndInfo**: スパンの完了時に出力されます。
- **LogRecordInfo**: スパン内のログイベントに対して出力されます。

## Usage Examples

注意：本稿執筆時点では、従来の `log` クレートから `tracing` クレートへの移行中です。ログ出力の大部分は、依然として `log!` ベースのマクロを介して行われています。

### 基本的なスパン計測

```rust
use tracing::{instrument, info_span};

// まれに、`#[instrument]` 属性を使用する代わりに、手動でスパンを作成する必要がある場合があります。
let session_span = tracing::info_span!(
    "Invocation",
    { TRACING_ATTR_FIELD } = SpanAttributes::Invocation {
        invocation_id: arg.io.invocation_id.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        host_os: std::env::consts::OS.to_string(),
        host_arch: std::env::consts::ARCH.to_string(),
        target: arg.target.clone(),
        metrics: None,
    }
    .to_tracing_value(),
);


// ただし、非同期境界には十分注意してください。
// 非同期境界を越えて使用される手動で作成されたスパンの複雑な使用例を確認するには、
// `crates/dbt-tasks/src/task_runner.rs` の `run_tasks_with_listener` と、
// `crates/dbt-tasks/src/runnable/mod.rs` の `run_task` のような特定のタスク内の
// 関連するネストされたスパンを参照してください。
```

### About async

関数がタスクを生成したり、非同期操作を待機したりする場合は、次のいずれかを行う必要があります。
- 非同期関数自体を `#[instrument]` でインストルメント化します。
- または、`.in_current_span()` または ` を使用して、span コンテキストが保持されるようにします。

```rust
use tracing::Instrument;

#[tracing::instrument(level = "trace")]
async fn parent_function() {
    // これは関数と同じ範囲で自動的に実行されます
    let result = child_function().in_current_span().await;
}

async fn non_instrumented() {
    let manual_span = tracing::info_span!("ManualSpan");

    // ここではスパンは入力されず、コードは親スパンで実行されます
    ...

    // しかし、非同期関数は手動スパンで入力して実行できます
    some_async_func().instrument(manual_span).await;
}
```

### 構造化イベントログ

すべてのデータを構造化属性としてトレースシステムに渡すために、常に特別な `TRACING_ATTR_FIELD` を使用していることに注意してください。

目標は、非構造化データをトレースシステムに渡さないことです。代わりに、すべてのデータを対応する `LogAttributes` バリアントに記録することで、色付きのttyログやプログレスバーなど、必要な出力形式を生成できるようになります。

```rust
tracing::warn!(
    { TRACING_ATTR_FIELD } = LogAttributes::Log {
        code: Some(err.code as u16 as u32),
        dbt_core_code: None,
        original_severity_number,
        original_severity_text,
        location: RecordCodeLocation::none(), // Will be auto injected
    }.to_tracing_value(),
    "{}",
    err.pretty().as_str()
);
```

## 開発者向けデバッグ機能

### 引数キャプチャを使用した開発者向けデバッグ

CLI 引数で `--log-level trace` が設定されている場合、トレースレベル範囲がキャプチャされます。
これはデバッグビルドと本番ビルドの両方で機能し、必要に応じてあらゆる環境で詳細なデバッグを行うことができます。

TRACE レベルでインストルメントされた関数は自動的に `SpanAttributes::DevInternal` になり、以下の情報を取得します。
- 関数名
- コードの場所 (ファイル、行、モジュール)
- 関数の引数 (--log-level trace が設定されている場合)
- カスタムデバッグフィールド

`#[instrument(level = "trace")]` を使用し、`--log-level trace` が設定されている場合、関数の引数が自動的にキャプチャされます。

```rust

// skip_allはすべての引数をスキップするために使用されることに注意してください
#[instrument(skip_all, level = "trace")]
fn my_function(arg1: &str, arg2: i32) -> Result<String, Error> {
    // すべての関数の引数は無視されますが、スパンが作成されます
    do_work(arg1, arg2)
}

#[instrument(skip(big_fat_arg), level = "trace")]
fn my_other_function(big_fat_arg: &MegaStruct, arg2: i32) -> Result<String, Error> {
    // --log-level トレースが設定されている場合、関数の引数がキャプチャされます。
    do_work(arg1, arg2)
}
```

## ログレベルのフィルタリング

トレース基盤は、デバッグビルドと本番ビルドの両方で `--log-level` CLI 引数を尊重し、デバッグビルドでは `RUST_LOG` 環境変数のみを尊重します。

```bash
# 開発者トレースを含むすべてのトレース出力を表示する
dbt --log-level trace run

# 特定のモジュールのスパンとイベントのみを表示する
RUST_LOG=dbt_tasks=debug,dbt_adapter=info dbt run

# エラーと警告のみを表示する
dbt --log-level warn run
```

**注**: `RUST_LOG` 環境変数はデバッグビルドでのみ考慮されます。本番ビルドでは、代わりに `--log-level` CLI 引数を使用してください。

## レガシーログブリッジ

インフラストラクチャには、既存の `log` クレート メッセージをトレース システムに転送するブリッジが含まれています。

### ブリッジ実装
- **場所**: `fs/sa/crates/dbt-common/src/logging/logger.rs`
- **目的**: レガシーログメッセージをキャプチャし、トレースに転送します
- **機能**:
    - ログレベルをトレースレベルに変換します
    - ANSIコードを削除して構造化された出力を作成します
    - ブリッジされたメッセージにマークを付け、二重処理を防止します
    - 移行中の下位互換性を維持します

### レガシーコードでの使用
```rust
use log::{info, error};

// これらは自動的に追跡に転送されます
info!("Legacy log message");
error!("Legacy error message");
```

## 構成

本稿執筆時点では、トレースは `otm_file_name` CLI 引数が設定されている場合にのみ有効になります。ログパスを使用して、テレメトリデータが JSONL ファイルに書き込まれます。

## ベストプラクティス

1. 下流で分析が必要なSpanには、**構造化属性を使用する**
2. 関数の場合は、手動でSpanを作成するよりも、**#[instrument]`**を使用する**
3. 引数キャプチャを含むSpanのデバッグには、**TRACEレベルを使用する**
4. 非同期関数ではないすべてのFutureと、Spanコンテキストを継承する必要があるすべての非同期操作には、**常に`.in_current_span()`**を使用する。非同期関数自体をインストルメント化することを推奨する。
5. **機密データを避ける** - `skip_all`を積極的に使用する

## 作業中

* メトリクスインフラストラクチャ
* OpenTelemetry の設定
* プログレスバーと CLI 出力へのブリッジ
