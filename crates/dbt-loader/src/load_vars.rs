use std::collections::BTreeMap;

use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::state::DbtVars;

// Load vars
// If no vars have been set, this is the root package and we need to set the global vars
// It's required that we push the "true" global vars to the vars vector, because these have
// not been expanded to consider the local package override.
// 変数が設定されていない場合、これはルート パッケージであり、グローバル変数を設定する必要があります。
// これらはローカル パッケージのオーバーライドを考慮して展開されていないため、「真の」グローバル変数を 
// vars ベクトルにプッシュする必要があります。
pub fn load_vars(
    package_name: &str,
    vars_val: Option<serde_json::Value>,
    collected_vars: &mut Vec<(String, BTreeMap<String, DbtVars>)>,
) -> FsResult<()> {
    // Check if vars are set on package
    // パッケージに変数が設定されているかどうかを確認する
    if let Some(package_vars_val) = vars_val {
        // Load vars from dbt_project.yml def
        // dbt_project.yml defから変数を読み込む
        let mut vars = serde_json::from_value::<BTreeMap<String, DbtVars>>(package_vars_val)
            .map_err(|e| {
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "Failed to parse variables for package {}: {}",
                    package_name,
                    e,
                )
            })?;
        // If no vars have been set yet, this is the root package and we need to set the global vars
        // まだ変数が設定されていない場合は、これはルートパッケージなので、グローバル変数を設定する必要があります。
        let global_vars = if collected_vars.is_empty() {
            collected_vars.push((package_name.to_string(), vars.clone()));
            BTreeMap::new()
        // Else, simply return the first element which is the global vars
        // それ以外の場合は、グローバル変数の最初の要素を返すだけです
        } else {
            collected_vars.first().unwrap().1.clone()
        };
        // If there are package vars, extend the vars with the package vars
        // パッケージ変数がある場合は、パッケージ変数で変数を拡張します
        if let Some(DbtVars::Vars(self_override)) = vars.get(package_name) {
            vars.extend(self_override.clone());
        }
        // Extend the vars with the global vars
        // グローバル変数で変数を拡張する
        vars.extend(global_vars.clone());
        // If there's a global var matching the package name and it's a BTreeMap, extend vars with it
        // パッケージ名に一致するグローバル変数があり、それがBTreeMapである場合は、それを使用して変数を拡張します。
        if let Some(DbtVars::Vars(global_package_vars)) = global_vars.get(package_name) {
            vars.extend(global_package_vars.clone());
        }
        collected_vars.push((package_name.to_string(), vars));
    // If package is not root (i.e. collected_vars is not empty) and package has no vars,
    // set the package vars to the global vars (first element of collected_vars)
    // パッケージがルートでない場合（つまり、collected_varsが空でない場合）、パッケージに変数がない場合、
    // パッケージの変数をグローバル変数（collected_varsの最初の要素）に設定します。
    } else if !collected_vars.is_empty() {
        let mut package_vars = collected_vars.first().unwrap().1.clone();
        if let Some(DbtVars::Vars(self_override)) = package_vars.get(package_name) {
            package_vars.extend(self_override.clone());
        }
        collected_vars.push((package_name.to_string(), package_vars))
    // If package is root and has no vars, push empty vars to collected_vars
    // パッケージがルートで変数がない場合は、空の変数を collected_vars にプッシュします。
    } else {
        collected_vars.push((package_name.to_string(), BTreeMap::new()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::state::DbtVars;
    use serde_json::json;

    #[test]
    fn test_load_vars() {
        // Macro from serde_json
        let root_package_name = "root_package";
        let child_package_name = "child_package";
        let global_vars = json!({
            "global_key": "global_value_parent",
            root_package_name: {
                "global_key": "global_value_parent_inner",
            },
            child_package_name: "global_package_ns_override",
        });

        let package_vars = json!({
            "global_key" : "child_global_value",
            child_package_name: {
                "global_key": "nested_child_global_value",
                "local_key": "nested_child_local_value",
            },
        });

        let mut collected_vars = vec![(
            root_package_name.to_string(),
            serde_json::from_value(global_vars).unwrap(),
        )];
        load_vars(
            child_package_name,
            Some(serde_json::from_value(package_vars).unwrap()),
            &mut collected_vars,
        )
        .expect("Failed to load vars");

        let expected = json!({
            child_package_name: "global_package_ns_override",
            "global_key": "global_value_parent",
            "local_key": "nested_child_local_value",
            root_package_name: {
                "global_key": "global_value_parent_inner",
            },
        });

        assert_eq!(
            serde_json::from_value::<BTreeMap<String, DbtVars>>(expected).unwrap(),
            collected_vars[1].1
        );
    }
}
