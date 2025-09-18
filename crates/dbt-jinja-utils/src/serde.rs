//! Provides the machineries to deserialize and process dbt YAML files.
//! dbt YAML ファイルをデシリアル化および処理するための仕組みを提供します。
//!
//! # Basic primitives
//!
//! * [`value_from_str()`]: this function creates a `yaml::Value` from a Yaml
//!   string, with proper warnings for duplicate keys
//! * [`into_typed_with_jinja<T>`]: this function consumes a `yaml::Value` to
//!   construct a `Deserialize` type `T`, while applying Jinja according to the
//!   rules encoded in `T`.
//!
//! There's also a shorthand, [`into_typed_raw<T>`], which is basically syntactic
//! sugar for `into_typed_with_jinja<Verbatim<T>>`.
//! 
//! # 基本的なプリミティブ
//! 
//! * [`value_from_str()`]: この関数は、Yaml 文字列から `yaml::Value` を作成し、
//!   重複キーに対して適切な警告を出力します。
//! * [`into_typed_with_jinja<T>`]: この関数は、`yaml::Value` を引数として 
//!   `Deserialize` 型の `T` を構築し、`T` にエンコードされたルールに従って Jinja を適用します。
//! 
//! [`into_typed_raw<T>`] という省略形もあります。
//! これは基本的に `into_typed_with_jinja<Verbatim<T>>` のシンタックスシュガーです。
//!
//! # Types
//!
//! * [`Omissible<T>`]: this is a wrapper type for use in
//!   `#[derive(Deserialize)]` structs, which allows you to distinguish between
//!   "omitted" and "explicit null" values.
//! 
//! # 型
//! 
//! * [`Omissible<T>`]: これは `#[derive(Deserialize)]` 構造体で使用するラッパー型で、
//!   これにより「省略された」値と「明示的な null」値を区別できます。
//!
//! # General usage guidelines
//!
//! * `yaml::Value` objects (and recursively all child `Value` objects)
//!   constructed by `value_from_str` are *fully self-contained with regards to
//!   source location*. This means that you can take a `Value`, pass it around,
//!   mix them up, then `into_typed` whenever you have the right Jinja context,
//!   and it's guaranteed to always raise errors with the correct location info.
//!   (i.e. you can use `yaml::Value` as ASTs for Yaml sources)
//!
//! * In `#[derive(Deserialize)]` schemas, use `Verbatim<Value>` for fields that
//!   require "deferred Jinja" processing; on the other hand, if the field
//!   should never be Jinja'd, you can directly `Verbatim` into the primitive
//!   type, e.g. `pub git: Verbatim<String>`
//!
//! * Avoid re-reading yaml files from disk for deferred-jinja -- you can now
//!   easily read to a `yaml::Value` for "raw" processing, and only apply Jinja
//!   when you have the full Jinja context
//!
//! * Avoid `json::Value` in Yaml structs -- we now have proper support for
//!   duplicate fields so there's no need to resort to `json::Value` to silently
//!   eat up duplicate fields.
//!
//! * Use the `dbt_serde_yaml::Spanned` wrapper type to capture the source
//!   location of any Yaml field.
//!
//! * `Option<Verbatim<T>>` does not work as expected due to implementation
//!   limitation -- always use `Verbatim<Option<T>>` instead.
//!
//! * Avoid using `#[serde(flatten)]` -- `Verbatim<T>` does not work with
//!   `#[serde(flatten)]`. Instead, use field names that starts and ends with
//!   `__` (e.g. `__additional_properties__`) -- all such named fields are
//!   flattened by `dbt_serde_yaml`, just as if they were annotated with
//!   `#[serde(flatten)]`. **NOTE** structs containing such fields will not
//!   serialize correctly with default serde serializers -- if you ever need to
//!   (re)serialize structs containing such fields, say into a
//!   `minijinja::Value`, serialize them to a `yaml::Value` *first*, then
//!   serialize the `yaml::Value` to the target format.
//!
//! * Untagged enums (`#[serde(untagged)]`) containing "magic" dbt-serde_yaml
//!   facilities, such as `Verbatim<T>` or `flatten_dunder` fields, does
//!   *not* work with the default `#[derive(Deserialize)]` decorator -- use
//!   `#[derive(UntaggedEnumDeserialize)]` instead (Note:
//!   `UntaggedEnumDeserialize` works on untagged enums *only* -- for all other
//!   types, use the default `#[derive(Deserialize)]` decorator).
//!
//! * For the specific use case of error recovery during deserialization, the
//!   `dbt_serde_yaml::ShouldBe<T>` wrapper type should be preferred -- unlike
//!   general `#[serde(untagged)]` enums which requires backtracking during
//!   deserialization, `ShouldBe<T>` does not backtrack and is zero overhead on
//!   the happy path (see type documentation for more details).
//! 
//! # 一般的な使用ガイドライン
//! 
//! * `value_from_str` によって構築される `yaml::Value` オブジェクト（および再帰的にすべての子 
//!   `Value` オブジェクト）は、*ソースの位置に関して完全に自己完結的です*。
//!   つまり、`Value` を受け取って、それを渡したり、組み合わせたり、適切な Jinja コンテキストがあれば 
//!   `into_typed` することができ、常に正しい位置情報でエラーが発生することが保証されます。
//!   (つまり、`yaml::Value` を Yaml ソースの AST として使用できます)
//! 
//! * `#[derive(Deserialize)]` スキーマでは、「遅延 Jinja」処理が必要なフィールドには 
//!   `Verbatim<Value>` を使用します。一方、フィールドを Jinja で処理する必要がない場合は、
//!   次のように、プリミティブ型に直接 `Verbatim` を代入できます。 `pub git: Verbatim<String>`
//! 
//! * deferred-jinja でディスクから Yaml ファイルを再読み込みしないようにします。
//!   `yaml::Value` に簡単に読み込んで「raw」処理を行い、完全な Jinja コンテキストがある場合にのみ 
//!   Jinja を適用できるようになりました。
//! 
//! * Yaml 構造体で `json::Value` を使用しないでください。
//!   重複フィールドが適切にサポートされるようになったため、重複フィールドを暗黙的に処理するために 
//!   `json::Value` を使用する必要がなくなりました。
//! 
//! * Yaml フィールドのソースコードの場所を取得するには、`dbt_serde_yaml::Spanned` ラッパー型を使用します。
//! 
//! * `Option<Verbatim<T>>` は実装上の制限により期待どおりに動作しません。
//!   代わりに常に `Verbatim<Option<T>>` を使用してください。
//! 
//! * `#[serde(flatten)]` の使用を避けます。`Verbatim<T>` は `#[serde(flatten)]` では動作しません。
//!   代わりに、`__` で始まって終わるフィールド名 (例: `__additional_properties__`) を使用してください。
//!   このような名前付きフィールドはすべて、`#[serde(flatten)]` で注釈が付けられているかのように、
//!   `dbt_serde_yaml` によってフラット化されます。
//!   **注意** 
//!   このようなフィールドを含む構造体は、デフォルトの serde シリアライザーでは正しくシリアル化されません。
//!   このようなフィールドを含む構造体を (再) シリアル化する必要がある場合 (たとえば、`minijinja::Value` 
//!   に)、*まず* `yaml::Value` にシリアル化し、次に `yaml::Value` をターゲット形式にシリアル化してください。
//! 
//! * `Verbatim<T>` や `flatten_dunder` フィールドなどの "magic" dbt-serde_yaml 機能を含むタグなし列挙型 
//!   (`#[serde(untagged)]`) は、デフォルトの `#[derive(Deserialize)]` デコレータでは動作しません。
//!   代わりに `#[derive(UntaggedEnumDeserialize)]` を使用してください
//!    (注: `UntaggedEnumDeserialize` はタグなし列挙型でのみ動作します。
//!   その他の型では、デフォルトの `#[derive(Deserialize)]` デコレータを使用してください)。
//! 
//! * デシリアライズ中にエラーを回復するという特定のユースケースでは、`dbt_serde_yaml::ShouldBe<T>` 
//!   ラッパー型を優先する必要があります。デシリアライズ中にバックトラックを必要とする一般的な 
//!   `#[serde(untagged)]` 列挙型とは異なり、`ShouldBe<T>` はバックトラックせず、ハッピー パスの
//!   オーバーヘッドはゼロです(詳細については、型のドキュメントを参照してください)。

use std::{
    path::{Path, PathBuf},
    rc::Rc,
    sync::LazyLock,
};

use dbt_common::{
    CodeLocation, ErrorCode, FsError, FsResult, fs_err, io_args::IoArgs,
    io_utils::try_read_yml_to_str, show_error, show_package_error, show_strict_error,
    show_warning_soon_to_be_error,
};
use dbt_serde_yaml::Value;
use minijinja::listener::RenderingEventListener;
use regex::Regex;
use serde::{Serialize, de::DeserializeOwned};

use crate::{jinja_environment::JinjaEnv, phases::load::secret_renderer::render_secrets};

pub use dbt_common::serde_utils::Omissible;

/// Deserializes a YAML file into a `Value`, using the file's absolute path for error reporting.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
/// 
/// エラー報告用のファイルの絶対パスを使用して、YAML ファイルを `Value` に逆シリアル化します。
/// 
/// `dependency_package_name` は、ファイルが依存パッケージの一部であるかどうかを判断するために使用され、
/// エラーの報告方法に影響します。
pub fn value_from_file(
    io_args: &IoArgs,
    path: &Path,
    show_errors_or_warnings: bool,
    dependency_package_name: Option<&str>,
) -> FsResult<Value> {
    let input = try_read_yml_to_str(path)?;
    value_from_str(
        io_args,
        &input,
        Some(path),
        show_errors_or_warnings,
        dependency_package_name,
    )
}

/// Renders a Yaml `Value` containing Jinja expressions into a target
/// `Deserialize` type T.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
/// 
/// Jinja 式を含む Yaml `Value` をターゲットの `Deserialize` 型 T にレンダリングします。
/// 
/// `dependency_package_name` は、ファイルが依存パッケージの一部であるかどうかを判断するために使用され、
/// エラーの報告方法に影響します。
pub fn into_typed_with_jinja<T, S>(
    io_args: &IoArgs,
    value: Value,
    should_render_secrets: bool,
    env: &JinjaEnv,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
    dependency_package_name: Option<&str>,
) -> FsResult<T>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let (res, errors) =
        into_typed_with_jinja_error(value, should_render_secrets, env, ctx, listeners)?;

    for error in errors {
        if let Some(package_name) = dependency_package_name
            && !io_args.show_all_deprecations
        {
            // If we are parsing a dependency package, we use a special macros
            // that ensures at most one error is shown per package.
            show_package_error!(io_args, package_name);
        } else {
            show_strict_error!(io_args, error, dependency_package_name);
        }
    }

    Ok(res)
}

/// Renders a Yaml `Value` containing Jinja expressions into a target
/// `Deserialize` type T.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
#[allow(clippy::too_many_arguments)]
pub fn into_typed_with_jinja_error_context<T, S>(
    io_args: Option<&IoArgs>,
    value: Value,
    should_render_secrets: bool,
    env: &JinjaEnv,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
    // A function that takes FsError and returns a string to be used as the error context
    error_context: impl Fn(&FsError) -> String,
    dependency_package_name: Option<&str>,
) -> FsResult<T>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let (res, errors) =
        into_typed_with_jinja_error(value, should_render_secrets, env, ctx, listeners)?;

    if let Some(io_args) = io_args {
        for error in errors {
            let context = error_context(&error);
            let error = error.with_context(context);
            if let Some(package_name) = dependency_package_name
                && !io_args.show_all_deprecations
            {
                // If we are parsing a dependency package, we use a special macros
                // that ensures at most one error is shown per package.
                show_package_error!(io_args, package_name);
            } else {
                show_strict_error!(io_args, error, dependency_package_name);
            }
        }
    }

    Ok(res)
}

/// Deserializes a Yaml `Value` into a target `Deserialize` type T.
pub fn into_typed_with_error<T>(
    io_args: &IoArgs,
    value: Value,
    show_errors_or_warnings: bool,
    dependency_package_name: Option<&str>,
    error_path: Option<PathBuf>,
) -> FsResult<T>
where
    T: DeserializeOwned,
{
    let (res, errors) = into_typed_internal(value, |_value| Ok(None))?;

    if show_errors_or_warnings {
        for error in errors {
            let error =
                error.with_location(CodeLocation::from(error_path.clone().unwrap_or_default()));
            if let Some(package_name) = dependency_package_name
                && !io_args.show_all_deprecations
            {
                // If we are parsing a dependency package, we use a special macros
                // that ensures at most one error is shown per package.
                show_package_error!(io_args, package_name);
            } else {
                show_strict_error!(io_args, error, dependency_package_name);
            }
        }
    }

    Ok(res)
}

/// Deserializes a Yaml string into a Rust type T.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
/// 
/// Yaml 文字列を Rust 型 T にデシリアライズします。
///
/// `dependency_package_name` は、ファイルが依存パッケージの一部であるかどうかを判断するために使用され、
/// エラーの報告方法に影響します。
pub fn from_yaml_raw<T>(
    io_args: &IoArgs,
    input: &str,
    error_display_path: Option<&Path>,
    show_errors_or_warnings: bool,
    dependency_package_name: Option<&str>,
) -> FsResult<T>
where
    T: DeserializeOwned,
{
    let value = value_from_str(
        io_args,
        input,
        error_display_path,
        show_errors_or_warnings,
        dependency_package_name,
    )?;
    // Use the identity transform for the 'raw' version of this function.
    // この関数の 'raw' バージョンには恒等変換を使用します。
    let expand_jinja = |_: &Value| Ok(None);

    let (res, errors) = into_typed_internal(value, expand_jinja)?;

    if show_errors_or_warnings {
        for error in errors {
            if let Some(package_name) = dependency_package_name
                && !io_args.show_all_deprecations
            {
                // If we are parsing a dependency package, we use a special macros
                // that ensures at most one error is shown per package.
                // 依存パッケージを解析する場合は、
                // パッケージごとに最大 1 つのエラーが表示されるようにする特別なマクロを使用します。
                show_package_error!(io_args, package_name);
            } else {
                show_strict_error!(io_args, error, dependency_package_name);
            }
        }
    }

    Ok(res)
}

fn detect_yaml_indentation(input: &str) -> Option<usize> {
    for line in input.lines() {
        if let Some((indentation, _)) = line.char_indices().find(|&(_, c)| !c.is_whitespace()) {
            if indentation == 2 || indentation == 4 {
                return Some(indentation);
            }
        }
    }

    None
}
fn replace_tabs_with_spaces(input: &str) -> String {
    // check if we have "\t"
    if input.contains("\t") {
        // detect the indentation spaces
        let indentation = detect_yaml_indentation(input).unwrap_or(2);
        input.replace("\t", &" ".repeat(indentation))
    } else {
        input.to_string()
    }
}

fn trim_beginning_whitespace_for_first_line_with_content(input: &str) -> String {
    let mut lines = input.lines();

    // Find the first line with content
    while let Some(line) = lines.next() {
        if line.trim().is_empty() {
            continue;
        }

        // Found a line with content, trim its beginning whitespace
        if let Some((whitespace_len, _)) = line.char_indices().find(|&(_, c)| !c.is_whitespace()) {
            // Return the first line with leading whitespace removed, followed by the rest of the input
            let rest_of_input = lines.collect::<Vec<&str>>().join("\n");
            if rest_of_input.is_empty() {
                return line[whitespace_len..].to_string();
            } else {
                return format!("{}\n{}", &line[whitespace_len..], rest_of_input);
            }
        }

        // If we get here, the line has content but no leading whitespace
        return input.to_string();
    }

    // If we get here, the input is empty or only contains empty lines
    input.to_string()
}

/// Internal function that deserializes a YAML string into a `Value`.
/// The error_display_path should be an absolute, canonicalized path.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
/// 
/// YAML 文字列を `Value` にデシリアライズする内部関数です。
/// error_display_path は、正規化された絶対パスである必要があります。
/// 
/// `dependency_package_name` は、ファイルが依存パッケージの一部であるかどうかを
/// 判断するために使用され、エラーの報告方法に影響します。
fn value_from_str(
    io_args: &IoArgs,
    input: &str,
    error_display_path: Option<&Path>,
    show_errors_or_warnings: bool,
    dependency_package_name: Option<&str>,
) -> FsResult<Value> {
    let _f = dbt_serde_yaml::with_filename(error_display_path.map(PathBuf::from));

    // replace tabs with spaces
    // trim beginning whitespace for the first line with content
    // タブをスペースに置き換えます
    // コンテンツを含む最初の行の先頭の空白を削除します
    let input = replace_tabs_with_spaces(input);
    let input = trim_beginning_whitespace_for_first_line_with_content(&input);
    let mut value = Value::from_str(&input, |path, key, existing_key| {
        let key_repr = dbt_serde_yaml::to_string(&key).unwrap_or_else(|_| "<opaque>".to_string());
        let path = strip_dunder_fields_from_path(&path.to_string());
        let duplicate_key_error = fs_err!(
            code => ErrorCode::DuplicateConfigKey,
            loc => key.span(),
            "Duplicate key `{}`. This key overwrites a previous definition of the same key \
                at line {} column {}. YAML path: `{}`.",
            key_repr.trim(),
            existing_key.span().start.line,
            existing_key.span().start.column,
            path
        );

        if show_errors_or_warnings {
            if let Some(package_name) = dependency_package_name
                && !io_args.show_all_deprecations
            {
                // If we are parsing a dependency package, we use a special macros
                // that ensures at most one error is shown per package.
                show_package_error!(io_args, package_name);
            } else {
                show_strict_error!(io_args, duplicate_key_error, dependency_package_name);
            }
        }
        // last key wins:
        dbt_serde_yaml::mapping::DuplicateKey::Overwrite
    })
    .map_err(|e| yaml_to_fs_error(e, error_display_path))?;
    value
        .apply_merge()
        .map_err(|e| yaml_to_fs_error(e, error_display_path))?;

    Ok(value)
}

/// Variant of into_typed_with_jinja which returns a Vec of warnings rather
/// than firing them.
fn into_typed_with_jinja_error<T, S>(
    value: Value,
    should_render_secrets: bool,
    env: &JinjaEnv,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
) -> FsResult<(T, Vec<FsError>)>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let jinja_renderer = |value: &Value| match value {
        Value::String(s, span) => {
            let expanded = render_jinja_str(s, should_render_secrets, env, ctx, listeners)
                .map_err(|e| e.with_location(span.clone()))?;
            Ok(Some(expanded.with_span(span.clone())))
        }
        _ => Ok(None),
    };

    into_typed_internal(value, jinja_renderer)
}

fn into_typed_internal<T, F>(value: Value, transform: F) -> FsResult<(T, Vec<FsError>)>
where
    T: DeserializeOwned,
    F: FnMut(&Value) -> Result<Option<Value>, Box<dyn std::error::Error + 'static + Send + Sync>>,
{
    let mut warnings: Vec<FsError> = Vec::new();
    let warn_unused_keys = |path: dbt_serde_yaml::path::Path, key: &Value, _: &Value| {
        let key_repr = dbt_serde_yaml::to_string(key).unwrap_or_else(|_| "<opaque>".to_string());
        let path = strip_dunder_fields_from_path(&path.to_string());
        warnings.push(*fs_err!(
            code => ErrorCode::UnusedConfigKey,
            loc => key.span(),
            "Ignored unexpected key `{:?}`. YAML path: `{}`.", key_repr.trim(), path
        ))
    };

    let res = value
        .into_typed(warn_unused_keys, transform)
        .map_err(|e| yaml_to_fs_error(e, None))?;
    Ok((res, warnings))
}

/// Strips any dunder fields (fields of the form `__<something>__`) from a dot-separated path string.
/// For example, "foo.__bar__.baz" becomes "foo.baz".
pub fn strip_dunder_fields_from_path(path: &str) -> String {
    path.split('.')
        .filter(|segment| {
            // Check if the segment is a dunder field: starts and ends with double underscores
            !(segment.starts_with("__") && segment.ends_with("__") && segment.len() > 4)
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Render a Jinja expression to a Value
fn render_jinja_str<S: Serialize>(
    s: &str,
    should_render_secrets: bool,
    env: &JinjaEnv,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
) -> FsResult<Value> {
    if check_single_expression_without_whitepsace_control(s) {
        let compiled = env.compile_expression(&s[2..s.len() - 2])?;
        let eval = compiled.eval(ctx, listeners)?;
        let val = dbt_serde_yaml::to_value(eval).map_err(|e| {
            yaml_to_fs_error(
                e,
                // The caller will attach the error location using the span in the
                // `Value` object, if available:
                None,
            )
        })?;
        let val = match val {
            Value::String(s, span) if should_render_secrets => {
                Value::string(render_secrets(s)?).with_span(span)
            }
            _ => val,
        };
        Ok(val)
    // Otherwise, process the entire string through Jinja
    } else {
        let compiled = env.render_str(s, ctx, listeners)?;
        let compiled = if should_render_secrets {
            render_secrets(compiled)?
        } else {
            compiled
        };
        Ok(Value::string(compiled))
    }
}

static RE_SIMPLE_EXPR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*\{\{\s*[^{}]+\s*\}\}\s*$").expect("valid regex"));

/// Check if the input is a single Jinja expression without whitespace control
pub fn check_single_expression_without_whitepsace_control(input: &str) -> bool {
    // The regex matches:
    //   ^\s*      -> optional whitespace at the beginning
    //   \{\{      -> the literal '{{'
    //   \s*       -> optional whitespace
    //   [^{}]+   -> one or more characters that are not '{', '}', or '-'
    //   \s*       -> optional whitespace
    //   \}\}      -> the literal '}}'
    //   \s*$      -> optional whitespace at the end
    !input.starts_with("{{-")
        && !input.ends_with("-}}")
        && input.starts_with("{{")
        && input.ends_with("}}")
        && { RE_SIMPLE_EXPR.is_match(input) }
}

/// Converts a `dbt_serde_yaml::Error` into a `FsError`, attaching the error location
pub fn yaml_to_fs_error(err: dbt_serde_yaml::Error, filename: Option<&Path>) -> Box<FsError> {
    let msg = err.display_no_mark().to_string();
    let location = err
        .span()
        .map_or_else(CodeLocation::default, CodeLocation::from);
    let location = if let Some(filename) = filename {
        location.with_file(filename)
    } else {
        location
    };

    if let Some(err) = err.into_external() {
        if let Ok(err) = err.downcast::<FsError>() {
            // These are errors raised from our own callbacks:
            return err;
        }
    }
    FsError::new(ErrorCode::SerializationError, format!("YAML error: {msg}"))
        .with_location(location)
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_single_expression_without_whitepsace_control() {
        assert!(check_single_expression_without_whitepsace_control(
            "{{ config(enabled=true) }}"
        ));
        assert!(!check_single_expression_without_whitepsace_control(
            "{{- config(enabled=true) -}}"
        ));
    }
}
