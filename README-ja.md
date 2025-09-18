<div style="text-align: center;">
  <img src="assets/dbt-fusion-engine.png" alt="dbt Fusion Engine" width="400" style="border-radius: 6px;" />
</div>

---

# dbt F✦SION engine (BETA)

このリポジトリは、`dbt` の将来のイノベーションの基盤となる dbt Fusion エンジンのコンポーネントをホストしています。dbt Fusion エンジンは Rust で記述されており、速度と正確性を重視して設計されており、複数のデータウェアハウス SQL 方言にわたる SQL をネイティブに理解します。

> [!IMPORTANT]  
> **Note: the dbt Fusion Engine is in Beta!**

dbt Core と比較した場合のバグや機能不足は、最終リリースに向けて継続的に解決されます（詳細については、[dbt Fusion エンジン：GA への道](https://docs.getdbt.com/blog/dbt-fusion-engine-path-to-ga) をご覧ください）。

dbt Fusion エンジンは、dbt Core 実行エンジンを根本から書き直したもので、標準の dbt オーサリングレイヤーとの相互運用性を確保するために構築されています。Fusion は、オーサリング仕様の曖昧な部分を dbt Core よりも厳密に適用し、正確性を確保します（たとえば、dbt Core はほとんどの YAML 構成をプロアクティブに検証しません）。これらの不一致の多くは、[dbt Autofix](https://github.com/dbt-labs/dbt-autofix) ツールによって自動的に修正できます。

dbt Core への準拠に加え、Fusion には新しい SQL 理解機能、言語サーバー、ウェアハウス接続用の最新の ADBC ドライバーなどが含まれています。 dbt Core は Python で記述されていますが、dbt Fusion エンジンは Rust で記述されており、単一のアプリケーションバイナリにコンパイルされています。

dbt-fusion は、ローカルマシン、Docker コンテナ、またはクラウド上のマシンにインストールできます。他のライブラリに依存しない柔軟なインストールを実現するように設計されています。dbt Fusion がロードするライブラリは、対応するデータベースドライバのみです。

dbt Fusion エンジンはこのリポジトリに段階的にリリースされるため、この注記が削除されるまで、このリポジトリにはエンジンのコア部分を動作させるクレートの一部のみが含まれています。これらのクレートは、5 月 28 日から段階的に公開されます。

## dbt Fusionエンジンを使い始める

> [!TIP]  
> 新しい dbt を使用するために、このプロジェクトをソースからビルドする必要はありません。追加機能を備えたプリコンパイル済みバイナリの使用をお勧めします。

Fusion を使い始めるにはいくつかの方法があります（詳細については、[dbt Fusion エンジンのクイックスタート](https://docs.getdbt.com/guides/fusion?step=1)をご覧ください）。
1. **dbt VS Code 拡張機能をダウンロード** - ほとんどのユーザーにとって最適な方法です。これにより、dbt fusion CLI と Language Server がシステムにインストールされます。ドキュメントページ [dbt VS Code 拡張機能のインストール](https://docs.getdbt.com/docs/install-dbt-extension) をご覧ください。
2. **Fusion を直接インストール** 以下のコマンドで fusion CLI のみをインストールするか、dbt のドキュメント [Fusion のインストールについて](https://docs.getdbt.com/docs/fusion/install-fusion) をご覧ください。

``` bash
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
```

3. **ソースから Fusion をビルドする** - 以下のセクションを参照してください: [ソースからのコンパイル](#compiling-from-source)


### サポートされているオペレーティングシステムとCPUマイクロアーキテクチャ

Fusion および関連ドライバは、CPU マイクロアーキテクチャとオペレーティングシステムごとに個別にコンパイルされます。これにより、ハードウェアレベルの最適化が可能になります。

凡例:
* 🟢 - 現在サポートされています
* 🟡 - 現在サポートされていません

| Operating System    | X86-64 | ARM  |
|-------------------|----------|------|
| MacOS             |   🟢     |  🟢  |
| Linux             |   🟢     |  🟢  |
| Windows           |   🟢     |  🟡  |


## タイムライン

| 目標日 | マイルストーン | 説明 |
|-------------|----------------------------|------------------------------------------|
| 2025-05-28 | Fusion の初期リリース | パーサー、スキーマ、dbt-jinja、Snowflake ADBC ドライバーのソースコードを公開しました。 |
| 2025-06-09 | Databricks アダプターのリリース | Databricks ADBC ドライバー、および Fusion 用アダプター |
| 2025-06-30 | BigQuery アダプターのリリース | BigQuery ADBC ドライバー、および Fusion 用アダプター |
| 2025-07-31 | Redshift アダプターのリリース | Redshift ADBC ドライバー、および Fusion 用アダプター |
| 2025-08-30 | ANTLR 文法のリリース + SQL パーサー | ANTLR パーサージェネレーターで使用される SQL 文法 |

### これまでにリリースされたトップレベルコンポーネント

さまざまな Fusion コンポーネントのリリースは、各コンポーネントが成熟し、貢献できる状態に達すると、反復的に行われます。

- [x] `dbt-jinja` - dbt の jinja 関数やその他の機能をサポートするための mini-jinja の Rust 拡張
- [x] `dbt-parser` - dbt プロジェクト用の Rust パーサー
- [x] `dbt-snowflake` - データベースドライバー
- [x] `dbt-schemas` - dbt のオーサリング サーフェス用の完全で正確な機械生成 JSON スキーマ
- [ ] `dbt-sql` - ANTLR文法と生成されたパーサー
  - [ ] snowflake.g4 
  - [ ] bigquery.g4
  - [ ] redshift.g4
  - [ ] databricks.g4
- [ ] Fusion: 包括的な dbt fusion エンジンのリリース。

## FAQ

<details>
  <summary><i>dbt Fusion エンジンに貢献できますか?</i></summary>

  はい、もちろんです！貢献ガイドラインについては[`CONTRIBUTING.md`](CONTRIBUTING.md)をご覧ください。
</details>

<details>
  <summary><i>dbt Fusion と dbt Core の違いは何ですか？</i></summary>
  dbt Fusion エンジンは、dbt Core を根本から書き直したもので、多くの機能が追加されています。
  *共通点:*
  * プロファイル、構成、シード、データテスト、ユニットテストを含む YML オーサリング形式
  * マテリアライゼーションライブラリ
  * dbt のライブラリ管理システム (ただし、`dbt deps` は自動的にインストールされます)

  *Fusion が提供する追加機能:*
  * 高速データ転送と統合接続処理を実現する、まったく新しい Arrow Database Connector (ADBC) ドライバー
  * 開発を容易にする言語サーバーと対応する VS-Code 拡張機能 (Cursor と互換性あり)
  * マルチダイアレクト SQL コンパイル、検証、静的解析
  * スタンドアロンディストリビューション。JVM や Python は不要です。
  * dbt パッケージやデータベースドライバーなどの依存関係は自動的にインストールされます。
  * dbt のコード署名済みで安全なディストリビューション
</details>

<details>
  <summary><i>このリポジトリには dbt のすべての機能が含まれていません。残りの機能はいつ公開されますか？</i></summary>
  dbt Fusion のソースコードは、コンポーネントが完成次第公開されます。上記のセクション [タイムライン](#timeline) をご覧ください。
</details>

<details>
  <summary><i>今日dbt Fusionを使用できますか?</i></summary>

  | 状態 | 説明 | 回避策 | 解決可能な担当者 |
  |--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------|---------------|
  | ブロック解除 | プロジェクトに変更を加えずに dbt Fusion エンジンを導入できます | --- | --- |
  | ソフト ブロック | プロジェクトに機能が含まれています (詳細については、[新しい dbt エンジンの準備方法](https://www.getdbt.com/blog/how-to-get-ready-for-the-new-dbt-engine) を参照してください)。 | dbt Studio の dbt-autofix スクリプトまたはワークフローを使用して、非推奨を解決してください | ユーザー |
  | ハード ブロック | プロジェクトに Python モデルが含まれているか、まだサポートされていないアダプターを使用しています | 可能な場合は、サポートされていない機能を削除してください | dbt Labs |

</details>



## ソースからのコンパイル

このリポジトリの主なCLIは`dbt-sa-cli`です。CLIをコンパイルするには、Rustツールチェーンが必要です。
まずはRustから始めましょう。以下のコマンドを実行して、マシンにRustをインストールしてください。

Linux:

```shell
sudo ./scripts/setup_dev_env_linux.sh
```

Mac:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

素晴らしい！Rustがインストールされました。確認するには、次のコマンドを実行してください。

```shell
cargo -v
```

次のような出力が表示されます:

```
Rust's package manager

Usage: cargo [+toolchain] [OPTIONS] [COMMAND]
       cargo [+toolchain] [OPTIONS] -Zscript <MANIFEST_RS> [ARGS]...

Options:
  -V, --version                  Print version info and exit
...
```

Cargo は Rust のビルドシステム兼パッケージマネージャーです。Python に慣れている方であれば、pip で十分でしょう。cargo を使ってコマンドを実行し、ローカルの `dbt-sa-cli` バイナリをビルドし、`cargo xtask` 経由でヘルパースクリプトを実行します。これについては後ほど詳しく説明します。

バイナリをローカルでビルドするには、`cd` でこのリポジトリのディレクトリに移動し、以下を実行します。

```shell
cargo build
```

これにより、Rustコードが`dbt-sa-cli`バイナリにコンパイルされます。完了すると、`target/debug/dbt-sa-cli`に新しい実行ファイルが作成されます。この実行ファイルは、CLIにパスを直接渡すことで実行できます。このgitリポジトリのルートディレクトリにいる場合は、以下を実行できます。

```shell
target/debug/dbt-sa-cli
```

正しくビルドされると、次のような出力が表示されます:

```shell
> ./target/debug/dbt
Usage: dbt <COMMAND>

Commands:
  parse    Parse models
  ...
```

なぜ`debug`ディレクトリにビルドされているのか疑問に思うかもしれません。これは、`cargo build`実行時のデフォルトプロファイルが`debug`であるためです。`debug`プロファイルはコードのコンパイル速度を速めますが、そのために最適化が犠牲になります。したがって、パーサーのベンチマークを行いたい場合は、`cargo build --release`フラグを付けてビルドしてください。コンパイル時間は長くなりますが、エンドユーザーのエクスペリエンスを再現したビルドになります。

この実行ファイルを頻繁に使用する場合は、`~/.zshrc`にエイリアスを作成することをお勧めします。エイリアスを作成するには、まず次のコマンドで実行ファイルへの絶対パスを取得します。

```shell
cd target/debug && pwd
```

## テストの実行

テストを実行するには、スタック サイズを増やして nextest を使用します。

```
 RUST_MIN_STACK=8388608 cargo nextest run --no-fail-fast
```

# ライセンス

dbt Fusion エンジンはモノレポジトリであり、複数のライセンスが含まれています。ほとんどのコードは ELv2 ライセンスです。詳細については、[`LICENSES.md`](LICENSES.md) をご覧ください。

# 謝辞

*dbtコミュニティの皆様へ:* dbtツールとdbt Labsは、素晴らしい開発者、貢献者、実践者、そして熱心な開発者からなるコミュニティのおかげで、今ここに存在しています。dbt Fusionは、その成果の進化形であり、これまでの成果の上に成り立っています。

*Arrowコミュニティの皆様へ:* dbt LabsはArrowエコシステムに全面的に取り組んでいます。Fusionは、ドライバーからアダプター、そしてコンパイラとランタイムの内部に至るまで、Arrow型システムのみを使用しています。

*DataFusionコミュニティの皆様へ:* SQLコンパイラの中間表現はDataFusion論理プランであり、実用的で拡張性が高く、あらゆる適切な方法で容易に操作できることが実証されています。

皆様、ありがとうございます。dbt、Arrow、そしてDataFusionは、真にグローバルなソフトウェアプロジェクトへと成長しました。dbt Labsは、今後数ヶ月、数年にわたり、これらの取り組みに意義ある貢献をしていくことをお約束します。
