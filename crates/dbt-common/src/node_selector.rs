//! Grammar for node selectors, used
//!     for --select, --exclude
//!     the result of evaluating --selectors .yml
//!     the result of normalizing any other node selection, e.g. --resource-types
//!
//! SelectExpression ::= AndSpecifiers | OrSpecifiers | AtomSpecifier
//! OrSpecifiers   ::= (SelectExpression)(' '+(SelectExpression))*
//! AndSpecifiers    ::= (SelectExpression)(','(SelectExpression)*)
//! AtomSpecifier   ::= Identifier | AtPattern | PlusPattern
//!
//! All three version of Atom Specifiers are define by the RE
//!     (?x)
//!      ^(?:([0-9]*)\+)?       # Optional leading number followed by plus     # PlusPattern
//!      (?:([a-zA-Z][a-zA-Z0-9._]*):)?                                        # Optional qualifier
//!      ([^, +@]+)              # Identifier, anything but blank, comma, plus # Identifier
//!      (?:\+([0-9]*))?$       # Optional trailing plus followed by number    # PlusPattern
//!      |                      # OR
//!      ^\@([A-Za-z0-9_]+)$                                                   # AtPattern

use dbt_serde_yaml::JsonSchema;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;
use strum::{Display, EnumIter, EnumString};

use crate::{ErrorCode, FsResult, err, fs_err};

// Common has only the syntax. The rest is in dbt-scheduler

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    EnumString,
    EnumIter,
    Display,
    Default,
    JsonSchema,
    Serialize,
    Deserialize,
)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
#[serde(rename_all = "lowercase")]
pub enum IndirectSelection {
    #[default]
    Eager,
    Buildable,
    Cautious,
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, EnumIter, Display, Deserialize)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum MethodName {
    Access,
    Config,
    Exposure,
    File,
    Fqn,
    Group,
    Metric,
    Package,
    Path,
    ResourceType,
    Result,
    SavedQuery,
    SemanticModel,
    Source,
    SourceStatus,
    State,
    Tag,
    TestName,
    TestType,
    UnitTest,
    Version,
    // Column selector
    // internal only to select column in a table
    // new syntax: column:<node_id>.<column_name>
    // todo: maybe better?: column:<node_id>#<column_name>
    Column,
}

impl MethodName {
    pub fn default_for(value: &str) -> Self {
        if value.contains(std::path::MAIN_SEPARATOR)
            || (std::path::MAIN_SEPARATOR != '/' && value.contains('/'))
        {
            Self::Path
        } else if value.to_ascii_lowercase().ends_with(".sql")
            || value.to_ascii_lowercase().ends_with(".py")
            || value.to_ascii_lowercase().ends_with(".csv")
        {
            Self::File
        } else {
            Self::Fqn
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct SelectionCriteria {
    // qualifier + optional sub‑parts ("config.materialized" ⇒ method="config", args=["materialized"])
    pub method: MethodName,
    pub method_args: Vec<String>,

    pub value: String, // the thing to match

    // graph‑walk modifiers
    pub childrens_parents: bool,     // `@`
    pub parents_depth: Option<u32>, // `+foo` or `N+foo` - None means no parents, Some(u32::MAX) means unlimited depth
    pub children_depth: Option<u32>, // `foo+` or `foo+N` - None means no children, Some(u32::MAX) means unlimited depth

    pub indirect: Option<IndirectSelection>,

    // nested excludes
    pub exclude: Option<Box<SelectExpression>>,
}

impl SelectionCriteria {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        method: MethodName,
        method_args: Vec<String>,
        value: String,
        childrens_parents: bool,
        parents_depth: Option<u32>,
        children_depth: Option<u32>,
        indirect: Option<IndirectSelection>,
        exclude: Option<Box<SelectExpression>>,
    ) -> Self {
        Self {
            method,
            method_args,
            value,
            childrens_parents,
            parents_depth,
            children_depth,
            indirect,
            exclude,
        }
    }
}

impl fmt::Display for SelectionCriteria {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::new();

        if self.childrens_parents {
            result.push('@');
        }

        if let Some(depth) = self.parents_depth {
            if depth > 0 {
                result.push_str(&depth.to_string());
            }
            result.push('+');
        }

        if !self.method_args.is_empty() {
            result.push_str(&format!("{}.", self.method.to_string().to_lowercase()));
            result.push_str(&self.method_args.join("."));
            result.push(':');
        } else {
            result.push_str(&format!("{}:", self.method.to_string().to_lowercase()));
        }

        result.push_str(&self.value);

        if let Some(depth) = self.children_depth {
            result.push('+');
            if depth > 0 {
                result.push_str(&depth.to_string());
            }
        }

        write!(f, "{result}")
    }
}

/// Represents the AST for model specifiers, which can be combined using logical AND and OR operations.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize)]
pub enum SelectExpression {
    Atom(SelectionCriteria),        // a single model specifier
    And(Vec<SelectExpression>),     // a list of model specifiers, joined by commas
    Or(Vec<SelectExpression>),      // a list of model specifiers, joined by spaces
    Exclude(Box<SelectExpression>), // For nested excludes
}

impl fmt::Display for SelectExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectExpression::And(expressions) => {
                let expressions_str = expressions
                    .iter()
                    .map(|expr| format!("{expr}"))
                    .collect::<Vec<_>>()
                    .join(",");
                write!(f, "{expressions_str}")
            }
            SelectExpression::Or(expressions) => {
                let expressions_str = expressions
                    .iter()
                    .map(|expr| format!("{expr}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                write!(f, "{expressions_str}")
            }
            SelectExpression::Atom(criteria) => write!(f, "{criteria}"),
            SelectExpression::Exclude(expr) => write!(f, "exclude({expr})"),
        }
    }
}

impl SelectExpression {
    /// Set the indirect selection mode for this expression and all nested expressions
    pub fn set_indirect_selection(&mut self, mode: IndirectSelection) {
        match self {
            SelectExpression::And(exprs) | SelectExpression::Or(exprs) => {
                for expr in exprs {
                    expr.set_indirect_selection(mode);
                }
            }
            SelectExpression::Atom(criteria) => {
                criteria.indirect = Some(mode);
            }
            SelectExpression::Exclude(expr) => {
                expr.set_indirect_selection(mode);
            }
        }
    }

    /// Apply default indirect selection mode to this expression and all nested expressions
    /// if not already specified
    pub fn apply_default_indirect_selection(&mut self, default_mode: IndirectSelection) {
        match self {
            SelectExpression::And(list) | SelectExpression::Or(list) => {
                for sub_expr in list {
                    sub_expr.apply_default_indirect_selection(default_mode);
                }
            }
            SelectExpression::Atom(criteria) => {
                // Only set indirect selection if not already specified
                if criteria.indirect.is_none() {
                    criteria.indirect = Some(default_mode);
                }
            }
            SelectExpression::Exclude(expr) => {
                expr.apply_default_indirect_selection(default_mode);
            }
        }
    }
}

// ------------------------------------------------------------------------------------------------
pub fn conjoin_expression(
    maybe_select_expression: Option<SelectExpression>,
    maybe_select_expression2: Option<SelectExpression>,
) -> Option<SelectExpression> {
    match (maybe_select_expression, maybe_select_expression2) {
        (Some(select), Some(select2)) => Some(SelectExpression::And(vec![select, select2])),
        (Some(select), None) => Some(select),
        (None, Some(select)) => Some(select),
        (None, None) => None,
    }
}

/// ----------------------------------
/// parsing
/// ----------------------------------
static RAW_SELECTOR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
^
(?P<childrens_parents>@)?
(?P<parents>(?P<parents_depth>\d*)\+)?   # optional leading +N
(?:(?P<method>[\w.]+):)?                 # optional qualifier
(?P<value>.*?)
(?P<children>\+(?P<children_depth>\d*) )?  # optional trailing +N
$
",
    )
    .unwrap()
});

pub fn parse_single_selector(raw: &str) -> FsResult<SelectionCriteria> {
    let caps = RAW_SELECTOR_RE
        .captures(raw)
        .ok_or_else(|| fs_err!(ErrorCode::SelectorError, "Invalid selector spec: `{}`", raw))?;

    // ------------------------------------------------------------------
    // Reject selectors whose <value> is empty (e.g. "++", "@", "+:").
    // ------------------------------------------------------------------
    let val = caps.name("value").unwrap().as_str();
    if val.is_empty() {
        return err!(ErrorCode::SelectorError, "Invalid selector spec: `{}`", raw);
    }

    // ---------------- method / args
    let (method, method_args) = if let Some(m) = caps.name("method") {
        let mut parts = m.as_str().split('.').map(|s| s.to_string());
        let head = parts.next().unwrap();
        let name = MethodName::from_str(&head).map_err(|_| {
            fs_err!(
                ErrorCode::SelectorError,
                "Invalid node selector method: `{}`",
                head
            )
        })?;
        (name, parts.collect())
    } else {
        let v = caps.name("value").unwrap().as_str();
        (MethodName::default_for(v), vec![])
    };

    // ---------------- depths & flags
    let parents_depth = if caps.name("parents").is_some() {
        caps.name("parents_depth")
            .map(|m| {
                if m.as_str().is_empty() {
                    u32::MAX
                } else {
                    m.as_str().parse::<u32>().unwrap()
                }
            })
            .or(Some(u32::MAX))
    } else {
        None
    };

    let children_depth = if caps.name("children").is_some() {
        caps.name("children_depth")
            .map(|m| {
                if m.as_str().is_empty() {
                    u32::MAX
                } else {
                    m.as_str().parse::<u32>().unwrap()
                }
            })
            .or(Some(u32::MAX))
    } else {
        None
    };

    let criteria = SelectionCriteria::new(
        method,
        method_args,
        caps.name("value").unwrap().as_str().to_string(),
        caps.name("childrens_parents").is_some(),
        parents_depth,
        children_depth,
        Some(IndirectSelection::default()), // CLI flag can override later
        None,
    );

    //---------------------------------------------------------------
    // `@foo+` is illegal
    //---------------------------------------------------------------
    if criteria.childrens_parents && criteria.children_depth.is_some() {
        return err!(
            ErrorCode::SelectorError,
            "Invalid selector `{}` - \"@\" and trailing \"+\" are incompatible",
            raw
        );
    }

    //---------------------------------------------------------------
    // `column:` must have a column name after the dot
    //     e.g.  column:node123.foo_col   ✔
    //           column:model.node123.col ✔
    //           column:node123.          ✘
    //---------------------------------------------------------------
    if criteria.method == MethodName::Column {
        // we only need to make sure there is *some* column name
        match criteria.value.rfind('.') {
            // a dot exists and it's not the last char ⇒ we're good
            Some(ix) if ix < criteria.value.len() - 1 => {}
            _ => {
                return err!(ErrorCode::SelectorError, "Invalid selector spec: `{}`", raw);
            }
        }
    }
    //---------------------------------------------------------------
    // If we see a '+' inside <value> but *didn't* set either the
    // `parents_depth` or `children_depth`, then the user wrote something
    // like  `identifier+abc`, i.e. a "depth" that isn't numeric.
    //---------------------------------------------------------------
    if criteria.parents_depth.is_none()
        && criteria.children_depth.is_none()
        && criteria.value.contains('+')
    {
        return err!(
            ErrorCode::SelectorError,
            "Invalid model specifier near: {}",
            raw
        );
    }

    Ok(criteria)
}

/// Turn the tokenised CLI list (already split on whitespace by Clap)
/// into a `SelectExpression` tree.
///
/// * **Outer level**: each whitespace‑separated token is an *OR* term.
/// * **Inner level**: inside each token, a comma `,` separates *AND* terms.
///
/// A few helper invariants make the logic easy to follow:
///
/// * A single criterion becomes `SelectExpression::Atom`.
/// * A list of N criteria joined by the same operator collapses to
///   `SelectExpression::And(vec)` or `SelectExpression::Or(vec)` where `vec.len() == N`.
/// * We never wrap a single Atom in an unnecessary `And/Or`.
/// 
/// トークン化された CLI リスト（Clap によって空白で分割済み）を `SelectExpression` ツリーに変換します。
///
/// * **外側のレベル**: 空白で区切られた各トークンは *OR* 項です。
/// * **内側のレベル**: 各トークン内では、カンマ `,` によって *AND* 項が区切られます。
///
/// いくつかの補助的な不変式により、ロジックの理解が容易になります。
/// 
/// * 単一の条件は `SelectExpression::Atom` になります。
/// * 同じ演算子で結合された N 個の条件のリストは、`SelectExpression::And(vec)` または 
///   `SelectExpression::Or(vec)` に集約されます。ここで、`vec.len() == N` です。
/// * 単一の Atom を不必要な `And/Or` で囲むことはありません。
pub fn parse_model_specifiers(tokens: &[String]) -> FsResult<SelectExpression> {
    if tokens.is_empty() {
        return Err(fs_err!(
            ErrorCode::SelectorError,
            "empty selector list passed to --select/--exclude"
        ));
    }

    // ----------- build OR level -------------------------------------------------------------
    let mut or_terms: Vec<SelectExpression> = Vec::new();

    for token in tokens {
        // Skip completely empty tokens (can happen after user writes two spaces in a row)
        if token.trim().is_empty() {
            continue;
        }

        // ----------- build AND level --------------------------------------------------------
        let mut and_terms: Vec<SelectExpression> = Vec::new();
        for piece in token.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let criteria = parse_single_selector(piece)?;
            and_terms.push(SelectExpression::Atom(criteria));
        }

        // collapse AND level
        let inner_expr = match and_terms.len() {
            0 => continue,                 // nothing in this token → ignore it
            1 => and_terms.pop().unwrap(), // single item – no extra wrapper
            _ => SelectExpression::And(and_terms),
        };

        or_terms.push(inner_expr);
    }

    // collapse OR level
    let root = match or_terms.len() {
        0 => {
            return Err(fs_err!(
                ErrorCode::SelectorError,
                "selector contained only delimiters but no actual criteria"
            ));
        }
        1 => or_terms.pop().unwrap(),
        _ => SelectExpression::Or(or_terms),
    };

    Ok(root)
}

/// helper used by the CLI to add an extra criterion (resource_type etc.)
pub fn conjoin_predicate(
    maybe_sel: Option<SelectExpression>,
    extra: Option<SelectionCriteria>,
) -> Option<SelectExpression> {
    match (maybe_sel, extra) {
        (None, None) => None,
        (Some(sel), None) => Some(sel),
        (None, Some(c)) => Some(SelectExpression::Atom(c)),
        (Some(sel), Some(c)) => {
            let atom = SelectExpression::Atom(c);
            Some(match sel {
                SelectExpression::And(mut v) => {
                    v.push(atom);
                    SelectExpression::And(v)
                }
                _ => SelectExpression::And(vec![sel, atom]),
            })
        }
    }
}

// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifier() -> FsResult<()> {
        let result = parse_single_selector("identifier")?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Fqn,
                method_args: vec![],
                value: "identifier".to_string(),
                childrens_parents: false,
                parents_depth: None,
                children_depth: None,
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_at_pattern() -> FsResult<()> {
        let result = parse_single_selector("@identifier")?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Fqn,
                method_args: vec![],
                value: "identifier".to_string(),
                childrens_parents: true,
                parents_depth: None,
                children_depth: None,
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_plus_pattern_leading_number() -> FsResult<()> {
        let result = parse_single_selector("2+identifier")?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Fqn,
                method_args: vec![],
                value: "identifier".to_string(),
                childrens_parents: false,
                parents_depth: Some(2),
                children_depth: None,
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_plus_pattern_trailing_plus() -> FsResult<()> {
        let result = parse_single_selector("identifier+")?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Fqn,
                method_args: vec![],
                value: "identifier".to_string(),
                childrens_parents: false,
                parents_depth: None,
                children_depth: Some(u32::MAX),
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_plus_pattern_trailing_number() -> FsResult<()> {
        let input = "identifier+8";
        let result = parse_model_specifiers(&[input.to_string()])?;
        assert_eq!(
            result,
            SelectExpression::Atom(SelectionCriteria::new(
                MethodName::Fqn,
                vec![],
                "identifier".to_string(),
                false,
                None,
                Some(8),
                Some(IndirectSelection::default()),
                None,
            ))
        );
        Ok(())
    }

    #[test]
    fn test_plus_pattern_both_plus() -> FsResult<()> {
        let input = "+identifier+";
        let result = parse_single_selector(input)?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Fqn,
                method_args: vec![],
                value: "identifier".to_string(),
                childrens_parents: false,
                parents_depth: Some(u32::MAX),
                children_depth: Some(u32::MAX),
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_plus_path_pattern_both_plus() -> FsResult<()> {
        let input = "+path:identifier/rest+";
        let result = parse_single_selector(input)?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Path,
                method_args: vec![],
                value: "identifier/rest".to_string(),
                childrens_parents: false,
                parents_depth: Some(u32::MAX),
                children_depth: Some(u32::MAX),
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_plus_pattern_leading_number_trailing_plus() -> FsResult<()> {
        let input = "5+identifier+";
        let result = parse_single_selector(input)?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Fqn,
                method_args: vec![],
                value: "identifier".to_string(),
                childrens_parents: false,
                parents_depth: Some(5),
                children_depth: Some(u32::MAX),
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_plus_pattern_leading_plus_trailing_number() -> FsResult<()> {
        let input = "+identifier+6";
        let result = parse_single_selector(input)?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Fqn,
                method_args: vec![],
                value: "identifier".to_string(),
                childrens_parents: false,
                parents_depth: Some(u32::MAX),
                children_depth: Some(6),
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_invalid_trailing_number() {
        let input = "identifier+abc";
        let result = parse_single_selector(input);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                e.to_string().trim(),
                "Invalid model specifier near: identifier+abc"
            );
        }
    }

    #[test]
    fn test_empty_string() {
        let input = "";
        let result = parse_single_selector(input);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.to_string().trim(), "Invalid selector spec: ``");
        }
    }

    #[test]
    fn test_only_plus_signs() {
        let input = "++";
        let result = parse_single_selector(input);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.to_string().trim(), "Invalid selector spec: `++`");
        }
    }

    #[test]
    fn invalid_at_pattern_with_plus() {
        let input = "@identifier+";
        let result = parse_single_selector(input);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                e.to_string().trim(),
                "Invalid selector `@identifier+` - \"@\" and trailing \"+\" are incompatible"
            );
        }
    }

    #[test]
    fn test_and_specifier() -> FsResult<()> {
        let input = vec!["identifier,@identifier".to_string()];
        let result = parse_model_specifiers(&input)?;
        match result {
            SelectExpression::And(vec) if vec.len() == 2 => match (&vec[0], &vec[1]) {
                (SelectExpression::Atom(x), SelectExpression::Atom(y)) => {
                    assert_eq!(
                        x,
                        &SelectionCriteria {
                            method: MethodName::Fqn,
                            method_args: vec![],
                            value: "identifier".to_string(),
                            childrens_parents: false,
                            parents_depth: None,
                            children_depth: None,
                            indirect: Some(IndirectSelection::default()),
                            exclude: None,
                        }
                    );
                    assert_eq!(
                        y,
                        &SelectionCriteria {
                            method: MethodName::Fqn,
                            method_args: vec![],
                            value: "identifier".to_string(),
                            childrens_parents: true,
                            parents_depth: None,
                            children_depth: None,
                            indirect: Some(IndirectSelection::default()),
                            exclude: None,
                        }
                    );
                }
                _ => panic!("Expected SelectExpr::Or variant with two Atom elements"),
            },
            _ => panic!("Expected SelectExpr::And variant with one element"),
        }

        Ok(())
    }

    #[test]
    fn test_or_specifier() -> FsResult<()> {
        let input = vec!["identifier".to_string(), "@identifier".to_string()];
        let result = parse_model_specifiers(&input)?;
        match result {
            SelectExpression::Or(vec) if vec.len() == 2 => match (&vec[0], &vec[1]) {
                (SelectExpression::Atom(x), SelectExpression::Atom(y)) => {
                    assert_eq!(
                        x,
                        &SelectionCriteria {
                            method: MethodName::Fqn,
                            method_args: vec![],
                            value: "identifier".to_string(),
                            childrens_parents: false,
                            parents_depth: None,
                            children_depth: None,
                            indirect: Some(IndirectSelection::default()),
                            exclude: None,
                        }
                    );
                    assert_eq!(
                        y,
                        &SelectionCriteria {
                            method: MethodName::Fqn,
                            method_args: vec![],
                            value: "identifier".to_string(),
                            childrens_parents: true,
                            parents_depth: None,
                            children_depth: None,
                            indirect: Some(IndirectSelection::default()),
                            exclude: None,
                        }
                    );
                }
                _ => panic!("Expected SelectExpr::Or variant with two Atom elements"),
            },
            _ => panic!("Expected SelectExpr::Or variant with two elements"),
        }

        Ok(())
    }

    #[test]
    fn test_column_selector_identifier() -> FsResult<()> {
        let input = "column:node123.foo_col";
        let result = parse_single_selector(input)?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Column,
                method_args: vec![],
                value: "node123.foo_col".to_string(),
                childrens_parents: false,
                parents_depth: None,
                children_depth: None,
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_column_selector_with_leading_plus() -> FsResult<()> {
        let input = "+column:node123.foo_col";
        let result = parse_single_selector(input)?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Column,
                method_args: vec![],
                value: "node123.foo_col".to_string(),
                childrens_parents: false,
                parents_depth: Some(u32::MAX),
                children_depth: None,
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_column_selector_with_trailing_plus() -> FsResult<()> {
        let input = "column:node123.foo_col+";
        let result = parse_single_selector(input)?;
        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Column,
                method_args: vec![],
                value: "node123.foo_col".to_string(),
                childrens_parents: false,
                parents_depth: None,
                children_depth: Some(u32::MAX),
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_column_selector_with_both_plus() -> FsResult<()> {
        let input = "+column:node123.foo_col+";
        let result = parse_single_selector(input)?;

        assert_eq!(
            result,
            SelectionCriteria {
                method: MethodName::Column,
                method_args: vec![],
                value: "node123.foo_col".to_string(),
                childrens_parents: false,
                parents_depth: Some(u32::MAX),
                children_depth: Some(u32::MAX),
                indirect: Some(IndirectSelection::default()),
                exclude: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_invalid_column_selector_with_missing_column_name() {
        let input = "column:node123.";
        let result = parse_single_selector(input);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                e.to_string().trim(),
                "Invalid selector spec: `column:node123.`"
            );
        }
    }

    #[test]
    fn test_column_selectors_with_trailing_plus() -> FsResult<()> {
        let input = vec![
            "column:node123.foo_col+".to_string(),
            "column:node123.bar_col+".to_string(),
        ];
        let result = parse_model_specifiers(&input)?;

        match result {
            SelectExpression::Or(vec) if vec.len() == 2 => match (&vec[0], &vec[1]) {
                (SelectExpression::Atom(a), SelectExpression::Atom(b)) => {
                    assert_eq!(
                        a,
                        &SelectionCriteria {
                            method: MethodName::Column,
                            method_args: vec![],
                            value: "node123.foo_col".to_string(),
                            childrens_parents: false,
                            parents_depth: None,
                            children_depth: Some(u32::MAX),
                            indirect: Some(IndirectSelection::default()),
                            exclude: None,
                        }
                    );
                    assert_eq!(
                        b,
                        &SelectionCriteria {
                            method: MethodName::Column,
                            method_args: vec![],
                            value: "node123.bar_col".to_string(),
                            childrens_parents: false,
                            parents_depth: None,
                            children_depth: Some(u32::MAX),
                            indirect: Some(IndirectSelection::default()),
                            exclude: None,
                        }
                    );
                }
                _ => panic!("Expected Atom variants"),
            },
            _ => panic!("Expected SelectExpr::Or expression with 2 atoms"),
        }

        Ok(())
    }
}
