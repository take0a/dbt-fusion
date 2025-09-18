use dbt_common::node_selector::IndirectSelection;
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
use dbt_common::{ErrorCode, FsResult, err, fs_err};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::parse::build_resolve_context;
use dbt_jinja_utils::serde::value_from_file;
use dbt_schemas::schemas::selectors::{SelectorEntry, SelectorFile};
use dbt_selector_parser::{ResolvedSelector, SelectorParser};
use std::collections::{BTreeMap, HashMap};

use crate::args::ResolveArgs;

/// Resolves selectors from YAML and computes the final include/exclude expressions.
/// This combines both parsing the selectors.yml file and computing the final expressions
/// that should be used by the scheduler.
///
/// The function:
/// 1. Parses and resolves selectors from YAML with Jinja templating
/// 2. Validates that only one selector is marked as default
/// 3. Computes the final include/exclude expressions based on:
///    - CLI selector flag or default selector
///    - Selector's include/exclude expressions
///    - CLI include/exclude flags
///    - CLI indirect selection mode (fallback if not specified in YAML)
///
/// Returns the final include and exclude expressions to be used by the scheduler.
/// 
/// YAMLからセレクタを解決し、最終的な包含/除外式を計算します。
/// これは、selectors.ymlファイルの解析と、スケジューラが使用する最終的な式の計算を組み合わせたものです。
///
/// 関数：
/// 1. Jinjaテンプレートを使用して、YAMLからセレクタを解析および解決します。
/// 2. デフォルトとしてマークされているセレクタが1つだけであることを検証します。
/// 3. 以下の基準に基づいて、最終的な包含/除外式を計算します。
///     - CLIセレクタフラグまたはデフォルトセレクタ
///     - セレクタの包含/除外式
///     - CLI包含/除外フラグ
///     - CLI間接選択モード（YAMLで指定されていない場合はフォールバック）
///
/// スケジューラが使用する最終的な包含/除外式を返します。
pub fn resolve_final_selectors(
    root_package_name: &str,
    jinja_env: &JinjaEnv,
    arg: &ResolveArgs,
) -> FsResult<ResolvedSelector> {
    let path = arg.io.in_dir.join("selectors.yml");
    if !path.exists() {
        // No YAML selectors - apply CLI indirect selection to any CLI select/exclude
        // YAMLセレクターなし - CLI間接選択を任意のCLI選択/除外に適用します
        let mut resolved = ResolvedSelector {
            include: arg.select.clone(),
            exclude: arg.exclude.clone(),
        };

        // Apply CLI indirect selection to both include and exclude expressions
        // CLI間接選択を包含式と除外式の両方に適用する
        if let Some(cli_mode) = arg.indirect_selection {
            if let Some(ref mut include) = resolved.include {
                include.apply_default_indirect_selection(cli_mode);
            }
            if let Some(ref mut exclude) = resolved.exclude {
                exclude.apply_default_indirect_selection(cli_mode);
            }
        }

        return Ok(resolved);
    }

    let raw_selectors = value_from_file(&arg.io, &path, true, None)?;

    let context = build_resolve_context(
        root_package_name,
        root_package_name,
        &BTreeMap::new(),
        DISPATCH_CONFIG.get().unwrap().read().unwrap().clone(),
    );

    // Parse and resolve selectors from YAML
    // YAML からセレクタを解析して解決する
    let yaml: SelectorFile = match dbt_jinja_utils::serde::into_typed_with_jinja(
        &arg.io,
        raw_selectors,
        false,
        jinja_env,
        &context,
        &[],
        None,
    ) {
        Ok(yaml) => yaml,
        Err(e) => {
            return err!(
                ErrorCode::SelectorError,
                "Error parsing selectors.yml: {}",
                e
            );
        }
    };

    // Build selector definitions map and resolve each selector
    // セレクタ定義マップを構築し、各セレクタを解決する
    let defs = yaml
        .selectors
        .iter()
        .map(|d| (d.name.clone(), d.clone()))
        .collect::<BTreeMap<_, _>>();
    let parser = SelectorParser::new(defs, &arg.io);
    let mut resolved_selectors = HashMap::new();
    for def in yaml.selectors {
        let resolved = parser.parse_definition(&def.definition)?;
        resolved_selectors.insert(
            def.name.clone(),
            SelectorEntry {
                include: resolved,
                is_default: def.default.unwrap_or(false),
                description: def.description,
            },
        );
    }

    // Validate only one default selector
    // デフォルトセレクタを1つだけ検証する
    if resolved_selectors.values().filter(|e| e.is_default).count() > 1 {
        return err!(
            ErrorCode::SelectorError,
            "Multiple selectors have `default: true`"
        );
    }

    // Find default selector name if no explicit selector provided
    // 明示的なセレクタが指定されていない場合はデフォルトのセレクタ名を検索する
    let default_sel_name = resolved_selectors.iter().find_map(|(name, entry)| {
        // Command line arguments (if provided) take precedence over the default
        if entry.is_default && !(arg.select.is_some() || arg.exclude.is_some()) {
            Some(name.clone())
        } else {
            None
        }
    });

    // Use explicit selector, default selector, or fall back to CLI flags
    // 明示的なセレクタ、デフォルトセレクタを使用するか、CLIフラグにフォールバックします
    if let Some(sel_name) = arg.selector.as_ref().or(default_sel_name.as_ref()) {
        // Look up selector and error if missing
        // セレクタを検索し、見つからない場合はエラーを表示します
        let entry = resolved_selectors.get(sel_name.as_str()).ok_or_else(|| {
            fs_err!(
                ErrorCode::SelectorError,
                "Unknown selector `{}` (see selectors.yml)",
                sel_name
            )
        })?;

        // Use selector's include and apply CLI indirect selection as fallback
        // セレクタの include を使用し、フォールバックとして CLI 間接選択を適用する
        let mut include = entry.include.clone();
        if let Some(cli_mode) = arg.indirect_selection {
            include.set_indirect_selection(cli_mode);
        }

        // Set exclude to CLI exclude and apply CLI indirect selection as fallback
        // 除外を CLI 除外に設定し、フォールバックとして CLI 間接選択を適用する
        let mut exclude = arg.exclude.clone();
        if let (Some(cli_mode), Some(exc)) = (arg.indirect_selection, exclude.as_mut()) {
            exc.set_indirect_selection(cli_mode);
        }

        Ok(ResolvedSelector {
            include: Some(include),
            exclude,
        })
    } else {
        // No selector chosen → use CLI flags and apply CLI indirect selection
        // セレクタが選択されていません → CLI フラグを使用し、CLI 間接選択を適用します
        let mut resolved = ResolvedSelector {
            include: arg.select.clone(),
            exclude: arg.exclude.clone(),
        };

        let default_mode = if arg.indirect_selection.is_some() {
            arg.indirect_selection.unwrap()
        } else {
            // eager is the default
            IndirectSelection::default()
        };

        if let Some(ref mut include) = resolved.include {
            include.apply_default_indirect_selection(default_mode);
        }
        if let Some(ref mut exclude) = resolved.exclude {
            exclude.apply_default_indirect_selection(default_mode);
        }

        Ok(resolved)
    }
}
