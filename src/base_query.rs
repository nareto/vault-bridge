use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use serde_yaml::Value as YamlValue;
use thiserror::Error;

use crate::model::NoteId;

const DEFAULT_BASE_QUERY_LIMIT: usize = 100;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueryBaseRequest {
    pub base_query: String,
    #[serde(default)]
    pub view: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryBaseResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
    pub columns: Vec<BaseQueryColumn>,
    pub rows: Vec<BaseQueryRow>,
    pub total: usize,
    pub returned: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BaseQueryColumn {
    pub id: String,
    pub label: String,
    pub value_type: BaseQueryValueType,
}

#[derive(Debug, Clone, Serialize)]
pub struct BaseQueryRow {
    pub note_id: NoteId,
    pub path: String,
    pub title: String,
    pub cells: JsonMap<String, JsonValue>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BaseQueryValueType {
    Empty,
    String,
    Number,
    Boolean,
    Array,
    Object,
    Date,
    Mixed,
}

#[derive(Debug, Clone)]
pub struct BaseQueryCandidate {
    pub id: NoteId,
    pub path: String,
    pub title: String,
    pub frontmatter: JsonValue,
    pub tags: Vec<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub links: Vec<String>,
}

#[derive(Debug, Error)]
pub enum BaseQueryError {
    #[error("base_query is required")]
    EmptyQuery,
    #[error("base query YAML is invalid: {0}")]
    InvalidYaml(#[from] serde_yaml::Error),
    #[error("base query must be a YAML mapping")]
    InvalidDocument,
    #[error("view not found: {0}")]
    ViewNotFound(String),
    #[error("unsupported base view type: {0}")]
    UnsupportedViewType(String),
    #[error("invalid base filter: {0}")]
    InvalidFilter(String),
    #[error("invalid base expression: {0}")]
    InvalidExpression(String),
    #[error("unsupported base expression: {0}")]
    UnsupportedExpression(String),
}

pub fn execute_query_base(
    request: QueryBaseRequest,
    mut candidates: Vec<BaseQueryCandidate>,
    max_limit: usize,
    now: DateTime<Utc>,
) -> Result<QueryBaseResponse, BaseQueryError> {
    let base_query = request.base_query.trim();
    if base_query.is_empty() {
        return Err(BaseQueryError::EmptyQuery);
    }

    let document = parse_base_document(base_query, request.view.as_deref())?;
    let max_limit = max_limit.max(1);
    let request_limit = request.limit.unwrap_or(max_limit).min(max_limit);
    let view_limit = document
        .view
        .limit
        .unwrap_or(DEFAULT_BASE_QUERY_LIMIT)
        .min(max_limit);
    let effective_limit = request_limit.min(view_limit);

    candidates.sort_by(|a, b| a.path.cmp(&b.path));
    if let Some(sort) = document.view.sort.as_ref() {
        candidates.sort_by(|a, b| compare_candidates(a, b, sort, now));
    }

    let mut matched_rows = Vec::new();
    for candidate in candidates {
        let context = EvalContext {
            candidate: &candidate,
            now,
        };
        if let Some(filter) = document.filter.as_ref()
            && !eval_filter(filter, &context)?
        {
            continue;
        }

        let mut cells = JsonMap::new();
        for property in &document.view.order {
            let value = evaluate_property_id(&property.id, &context)?;
            cells.insert(property.id.clone(), value.into_json());
        }
        matched_rows.push(BaseQueryRow {
            note_id: candidate.id,
            path: candidate.path,
            title: candidate.title,
            cells,
        });
    }

    let total = matched_rows.len();
    let column_types = document
        .view
        .order
        .iter()
        .map(|property| {
            (
                property.id.clone(),
                infer_column_type(&matched_rows, &property.id),
            )
        })
        .collect::<HashMap<_, _>>();
    let rows = matched_rows
        .into_iter()
        .take(effective_limit)
        .collect::<Vec<_>>();
    let columns = document
        .view
        .order
        .into_iter()
        .map(|property| {
            let value_type = column_types
                .get(&property.id)
                .copied()
                .unwrap_or(BaseQueryValueType::Empty);
            let label = document
                .properties
                .get(&property.id)
                .and_then(|property| property.display_name.clone())
                .unwrap_or_else(|| default_property_label(&property.id));
            BaseQueryColumn {
                id: property.id,
                label,
                value_type,
            }
        })
        .collect::<Vec<_>>();

    Ok(QueryBaseResponse {
        view: document.view.name,
        columns,
        returned: rows.len(),
        truncated: total > rows.len(),
        total,
        rows,
    })
}

#[derive(Debug, Clone)]
struct ParsedBaseDocument {
    filter: Option<FilterNode>,
    properties: HashMap<String, BasePropertyConfig>,
    view: ParsedBaseView,
}

#[derive(Debug, Clone)]
struct ParsedBaseView {
    name: Option<String>,
    order: Vec<PropertyRef>,
    limit: Option<usize>,
    sort: Option<Vec<SortSpec>>,
}

#[derive(Debug, Clone)]
struct BasePropertyConfig {
    display_name: Option<String>,
}

#[derive(Debug, Clone)]
struct PropertyRef {
    id: String,
}

#[derive(Debug, Clone)]
struct SortSpec {
    property_id: String,
    direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
struct RawBaseView {
    name: Option<String>,
    view_type: Option<String>,
    filter: Option<FilterNode>,
    order: Vec<PropertyRef>,
    limit: Option<usize>,
    sort: Option<Vec<SortSpec>>,
}

fn parse_base_document(
    raw: &str,
    requested_view: Option<&str>,
) -> Result<ParsedBaseDocument, BaseQueryError> {
    let value = serde_yaml::from_str::<YamlValue>(raw)?;
    let root = value.as_mapping().ok_or(BaseQueryError::InvalidDocument)?;

    let global_filter = yaml_key(root, "filters")
        .map(parse_filter_node)
        .transpose()?;
    let properties = yaml_key(root, "properties")
        .map(parse_properties)
        .transpose()?
        .unwrap_or_default();
    let views = yaml_key(root, "views")
        .map(parse_views)
        .transpose()?
        .unwrap_or_default();

    let raw_view = choose_view(views, requested_view)?;
    let filter = match (global_filter, raw_view.filter) {
        (Some(global), Some(view)) => Some(FilterNode::And(vec![global, view])),
        (Some(global), None) => Some(global),
        (None, Some(view)) => Some(view),
        (None, None) => None,
    };
    let order = if raw_view.order.is_empty() {
        default_order()
    } else {
        raw_view.order
    };

    Ok(ParsedBaseDocument {
        filter,
        properties,
        view: ParsedBaseView {
            name: raw_view.name,
            order,
            limit: raw_view.limit,
            sort: raw_view.sort,
        },
    })
}

fn choose_view(
    views: Vec<RawBaseView>,
    requested_view: Option<&str>,
) -> Result<RawBaseView, BaseQueryError> {
    if let Some(requested) = requested_view
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let view = views
            .into_iter()
            .find(|view| view.name.as_deref() == Some(requested))
            .ok_or_else(|| BaseQueryError::ViewNotFound(requested.to_string()))?;
        ensure_table_view(&view)?;
        return Ok(view);
    }

    if views.is_empty() {
        return Ok(RawBaseView {
            name: None,
            view_type: Some("table".to_string()),
            filter: None,
            order: default_order(),
            limit: None,
            sort: None,
        });
    }

    views
        .into_iter()
        .find(is_table_view)
        .ok_or_else(|| BaseQueryError::UnsupportedViewType("no table view found".to_string()))
}

fn ensure_table_view(view: &RawBaseView) -> Result<(), BaseQueryError> {
    if is_table_view(view) {
        Ok(())
    } else {
        Err(BaseQueryError::UnsupportedViewType(
            view.view_type
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
        ))
    }
}

fn is_table_view(view: &RawBaseView) -> bool {
    view.view_type
        .as_deref()
        .is_none_or(|view_type| view_type.eq_ignore_ascii_case("table"))
}

fn parse_properties(
    value: &YamlValue,
) -> Result<HashMap<String, BasePropertyConfig>, BaseQueryError> {
    let Some(map) = value.as_mapping() else {
        return Err(BaseQueryError::InvalidDocument);
    };
    let mut properties = HashMap::new();
    for (key, value) in map {
        let Some(raw_id) = key.as_str() else {
            continue;
        };
        let id = normalize_property_id(raw_id)?;
        let display_name = value
            .as_mapping()
            .and_then(|config| yaml_key(config, "displayName"))
            .and_then(YamlValue::as_str)
            .map(ToString::to_string);
        properties.insert(id, BasePropertyConfig { display_name });
    }
    Ok(properties)
}

fn parse_views(value: &YamlValue) -> Result<Vec<RawBaseView>, BaseQueryError> {
    let Some(items) = value.as_sequence() else {
        return Err(BaseQueryError::InvalidDocument);
    };
    items.iter().map(parse_view).collect()
}

fn parse_view(value: &YamlValue) -> Result<RawBaseView, BaseQueryError> {
    let Some(map) = value.as_mapping() else {
        return Err(BaseQueryError::InvalidDocument);
    };
    let view_type = yaml_key(map, "type")
        .and_then(YamlValue::as_str)
        .map(ToString::to_string);
    let name = yaml_key(map, "name")
        .and_then(YamlValue::as_str)
        .map(ToString::to_string);
    let filter = yaml_key(map, "filters")
        .map(parse_filter_node)
        .transpose()?;
    let order = yaml_key(map, "order")
        .map(parse_order)
        .transpose()?
        .unwrap_or_default();
    let limit = yaml_key(map, "limit")
        .and_then(YamlValue::as_u64)
        .map(|value| value as usize);
    let sort = yaml_key(map, "sort")
        .or_else(|| yaml_key(map, "sortBy"))
        .map(parse_sort)
        .transpose()?;

    Ok(RawBaseView {
        name,
        view_type,
        filter,
        order,
        limit,
        sort,
    })
}

fn parse_order(value: &YamlValue) -> Result<Vec<PropertyRef>, BaseQueryError> {
    let Some(items) = value.as_sequence() else {
        return Err(BaseQueryError::InvalidDocument);
    };
    items
        .iter()
        .map(|item| {
            let raw = if let Some(raw) = item.as_str() {
                raw
            } else if let Some(map) = item.as_mapping() {
                yaml_key(map, "property")
                    .and_then(YamlValue::as_str)
                    .ok_or(BaseQueryError::InvalidDocument)?
            } else {
                return Err(BaseQueryError::InvalidDocument);
            };
            Ok(PropertyRef {
                id: normalize_property_id(raw)?,
            })
        })
        .collect()
}

fn parse_sort(value: &YamlValue) -> Result<Vec<SortSpec>, BaseQueryError> {
    match value {
        YamlValue::Sequence(items) => items.iter().map(parse_sort_item).collect(),
        YamlValue::Mapping(_) | YamlValue::String(_) => Ok(vec![parse_sort_item(value)?]),
        _ => Err(BaseQueryError::InvalidDocument),
    }
}

fn parse_sort_item(value: &YamlValue) -> Result<SortSpec, BaseQueryError> {
    if let Some(raw) = value.as_str() {
        return Ok(SortSpec {
            property_id: normalize_property_id(raw)?,
            direction: SortDirection::Asc,
        });
    }

    let Some(map) = value.as_mapping() else {
        return Err(BaseQueryError::InvalidDocument);
    };
    let raw_property = yaml_key(map, "property")
        .and_then(YamlValue::as_str)
        .ok_or(BaseQueryError::InvalidDocument)?;
    let direction = yaml_key(map, "direction")
        .or_else(|| yaml_key(map, "order"))
        .and_then(YamlValue::as_str)
        .map(parse_sort_direction)
        .transpose()?
        .unwrap_or(SortDirection::Asc);

    Ok(SortSpec {
        property_id: normalize_property_id(raw_property)?,
        direction,
    })
}

fn parse_sort_direction(raw: &str) -> Result<SortDirection, BaseQueryError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "asc" | "ascending" => Ok(SortDirection::Asc),
        "desc" | "descending" => Ok(SortDirection::Desc),
        other => Err(BaseQueryError::InvalidFilter(format!(
            "unsupported sort direction '{other}'"
        ))),
    }
}

fn default_order() -> Vec<PropertyRef> {
    vec![
        PropertyRef {
            id: "file.name".to_string(),
        },
        PropertyRef {
            id: "file.path".to_string(),
        },
    ]
}

fn yaml_key<'a>(map: &'a serde_yaml::Mapping, key: &str) -> Option<&'a YamlValue> {
    map.get(YamlValue::String(key.to_string()))
}

fn normalize_property_id(raw: &str) -> Result<String, BaseQueryError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(BaseQueryError::InvalidDocument);
    }
    if trimmed.starts_with("file.")
        || trimmed.starts_with("note.")
        || trimmed.starts_with("formula.")
    {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("note.{trimmed}"))
    }
}

#[derive(Debug, Clone)]
enum FilterNode {
    Expr(Expr),
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Not(Box<FilterNode>),
}

fn parse_filter_node(value: &YamlValue) -> Result<FilterNode, BaseQueryError> {
    if let Some(statement) = value.as_str() {
        return Ok(FilterNode::Expr(parse_expression(statement)?));
    }

    let Some(map) = value.as_mapping() else {
        return Err(BaseQueryError::InvalidFilter(
            "filter must be a string or an and/or/not mapping".to_string(),
        ));
    };

    let mut logical_entries = Vec::new();
    for key in ["and", "or", "not"] {
        if let Some(value) = yaml_key(map, key) {
            logical_entries.push((key, value));
        }
    }
    if logical_entries.len() != 1 {
        return Err(BaseQueryError::InvalidFilter(
            "filter mapping must contain exactly one of and, or, or not".to_string(),
        ));
    }

    let (operator, value) = logical_entries[0];
    match operator {
        "and" => Ok(FilterNode::And(parse_filter_list(value)?)),
        "or" => Ok(FilterNode::Or(parse_filter_list(value)?)),
        "not" => {
            let children = parse_filter_list_or_single(value)?;
            let child = if children.len() == 1 {
                children.into_iter().next().expect("one child")
            } else {
                FilterNode::And(children)
            };
            Ok(FilterNode::Not(Box::new(child)))
        }
        _ => unreachable!(),
    }
}

fn parse_filter_list(value: &YamlValue) -> Result<Vec<FilterNode>, BaseQueryError> {
    let Some(items) = value.as_sequence() else {
        return Err(BaseQueryError::InvalidFilter(
            "and/or filters must be YAML lists".to_string(),
        ));
    };
    items.iter().map(parse_filter_node).collect()
}

fn parse_filter_list_or_single(value: &YamlValue) -> Result<Vec<FilterNode>, BaseQueryError> {
    if value.as_sequence().is_some() {
        parse_filter_list(value)
    } else {
        Ok(vec![parse_filter_node(value)?])
    }
}

fn eval_filter(node: &FilterNode, context: &EvalContext<'_>) -> Result<bool, BaseQueryError> {
    match node {
        FilterNode::Expr(expr) => Ok(eval_expr(expr, context)?.truthy()),
        FilterNode::And(children) => {
            for child in children {
                if !eval_filter(child, context)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        FilterNode::Or(children) => {
            for child in children {
                if eval_filter(child, context)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        FilterNode::Not(child) => Ok(!eval_filter(child, context)?),
    }
}

#[derive(Debug, Clone)]
enum Expr {
    Literal(EvalValue),
    Identifier(String),
    Member(Box<Expr>, String),
    Index(Box<Expr>, Box<Expr>),
    Call(Box<Expr>, Vec<Expr>),
    UnaryNot(Box<Expr>),
    Binary(Box<Expr>, BinaryOp, Box<Expr>),
}

#[derive(Debug, Clone, Copy)]
enum BinaryOp {
    Or,
    And,
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq)]
enum EvalValue {
    Missing,
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Date(DateTime<Utc>),
    Array(Vec<EvalValue>),
    Object(HashMap<String, EvalValue>),
    File,
    Note,
    Formula,
}

impl EvalValue {
    fn truthy(&self) -> bool {
        match self {
            EvalValue::Missing | EvalValue::Null => false,
            EvalValue::Bool(value) => *value,
            EvalValue::Number(value) => *value != 0.0,
            EvalValue::String(value) => !value.is_empty(),
            EvalValue::Date(_) => true,
            EvalValue::Array(value) => !value.is_empty(),
            EvalValue::Object(value) => !value.is_empty(),
            EvalValue::File | EvalValue::Note | EvalValue::Formula => true,
        }
    }

    fn into_json(self) -> JsonValue {
        match self {
            EvalValue::Missing
            | EvalValue::Null
            | EvalValue::File
            | EvalValue::Note
            | EvalValue::Formula => JsonValue::Null,
            EvalValue::Bool(value) => JsonValue::Bool(value),
            EvalValue::Number(value) => Number::from_f64(value)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            EvalValue::String(value) => JsonValue::String(value),
            EvalValue::Date(value) => JsonValue::String(value.to_rfc3339()),
            EvalValue::Array(values) => {
                JsonValue::Array(values.into_iter().map(EvalValue::into_json).collect())
            }
            EvalValue::Object(values) => JsonValue::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, value.into_json()))
                    .collect(),
            ),
        }
    }
}

struct EvalContext<'a> {
    candidate: &'a BaseQueryCandidate,
    now: DateTime<Utc>,
}

fn eval_expr(expr: &Expr, context: &EvalContext<'_>) -> Result<EvalValue, BaseQueryError> {
    match expr {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Identifier(name) => eval_identifier(name, context),
        Expr::Member(object, field) => {
            let value = eval_expr(object, context)?;
            eval_member(value, field, context)
        }
        Expr::Index(object, index) => {
            let value = eval_expr(object, context)?;
            let index = eval_expr(index, context)?;
            eval_index(value, index, context)
        }
        Expr::Call(callee, args) => eval_call(callee, args, context),
        Expr::UnaryNot(inner) => Ok(EvalValue::Bool(!eval_expr(inner, context)?.truthy())),
        Expr::Binary(left, operator, right) => eval_binary(left, *operator, right, context),
    }
}

fn eval_identifier(name: &str, context: &EvalContext<'_>) -> Result<EvalValue, BaseQueryError> {
    match name {
        "file" => Ok(EvalValue::File),
        "note" => Ok(EvalValue::Note),
        "formula" => Ok(EvalValue::Formula),
        "true" => Ok(EvalValue::Bool(true)),
        "false" => Ok(EvalValue::Bool(false)),
        "null" => Ok(EvalValue::Null),
        other => Ok(frontmatter_property(&context.candidate.frontmatter, other)),
    }
}

fn eval_member(
    value: EvalValue,
    field: &str,
    context: &EvalContext<'_>,
) -> Result<EvalValue, BaseQueryError> {
    match value {
        EvalValue::File => file_property(context.candidate, field),
        EvalValue::Note => Ok(frontmatter_property(&context.candidate.frontmatter, field)),
        EvalValue::Formula => Err(BaseQueryError::UnsupportedExpression(
            "formula properties are not supported yet".to_string(),
        )),
        EvalValue::Object(values) => Ok(values.get(field).cloned().unwrap_or(EvalValue::Missing)),
        EvalValue::Missing | EvalValue::Null => Ok(EvalValue::Missing),
        other => Err(BaseQueryError::UnsupportedExpression(format!(
            "cannot read member '{field}' from {}",
            value_kind(&other)
        ))),
    }
}

fn eval_index(
    value: EvalValue,
    index: EvalValue,
    context: &EvalContext<'_>,
) -> Result<EvalValue, BaseQueryError> {
    match (value, index) {
        (EvalValue::Note, EvalValue::String(field)) => {
            Ok(frontmatter_property(&context.candidate.frontmatter, &field))
        }
        (EvalValue::Object(values), EvalValue::String(field)) => {
            Ok(values.get(&field).cloned().unwrap_or(EvalValue::Missing))
        }
        (EvalValue::Array(values), EvalValue::Number(index))
            if index.fract() == 0.0 && index >= 0.0 =>
        {
            Ok(values
                .get(index as usize)
                .cloned()
                .unwrap_or(EvalValue::Missing))
        }
        (EvalValue::Missing | EvalValue::Null, _) => Ok(EvalValue::Missing),
        (value, index) => Err(BaseQueryError::UnsupportedExpression(format!(
            "cannot index {} with {}",
            value_kind(&value),
            value_kind(&index)
        ))),
    }
}

fn eval_call(
    callee: &Expr,
    args: &[Expr],
    context: &EvalContext<'_>,
) -> Result<EvalValue, BaseQueryError> {
    if let Expr::Identifier(name) = callee {
        return eval_global_call(name, args, context);
    }

    if let Expr::Member(object, method) = callee {
        if let Expr::Identifier(root) = object.as_ref()
            && root == "file"
        {
            return eval_file_method(method, args, context);
        }
        let target = eval_expr(object, context)?;
        return eval_value_method(target, method, args, context);
    }

    Err(BaseQueryError::UnsupportedExpression(
        "unsupported function call".to_string(),
    ))
}

fn eval_global_call(
    name: &str,
    args: &[Expr],
    context: &EvalContext<'_>,
) -> Result<EvalValue, BaseQueryError> {
    match name {
        "list" => {
            let [arg] = args else {
                return Err(BaseQueryError::InvalidExpression(
                    "list() expects exactly one argument".to_string(),
                ));
            };
            let value = eval_expr(arg, context)?;
            Ok(match value {
                EvalValue::Missing | EvalValue::Null => EvalValue::Array(Vec::new()),
                EvalValue::Array(_) => value,
                other => EvalValue::Array(vec![other]),
            })
        }
        "date" => {
            let [arg] = args else {
                return Err(BaseQueryError::InvalidExpression(
                    "date() expects exactly one argument".to_string(),
                ));
            };
            let value = eval_expr(arg, context)?;
            let Some(raw) = value_as_string(&value) else {
                return Err(BaseQueryError::InvalidExpression(
                    "date() expects a string argument".to_string(),
                ));
            };
            parse_date_value(&raw)
                .map(EvalValue::Date)
                .ok_or_else(|| BaseQueryError::InvalidExpression(format!("invalid date '{raw}'")))
        }
        "now" => {
            if !args.is_empty() {
                return Err(BaseQueryError::InvalidExpression(
                    "now() expects no arguments".to_string(),
                ));
            }
            Ok(EvalValue::Date(context.now))
        }
        "today" => {
            if !args.is_empty() {
                return Err(BaseQueryError::InvalidExpression(
                    "today() expects no arguments".to_string(),
                ));
            }
            let today = context.now.date_naive();
            let start = today
                .and_hms_opt(0, 0, 0)
                .expect("valid start of day")
                .and_utc();
            Ok(EvalValue::Date(start))
        }
        other => Err(BaseQueryError::UnsupportedExpression(format!(
            "unsupported function '{other}'"
        ))),
    }
}

fn eval_file_method(
    method: &str,
    args: &[Expr],
    context: &EvalContext<'_>,
) -> Result<EvalValue, BaseQueryError> {
    match method {
        "hasTag" => {
            let needle = single_string_argument("file.hasTag", args, context)?;
            let normalized = needle.trim_start_matches('#');
            Ok(EvalValue::Bool(context.candidate.tags.iter().any(|tag| {
                tag == normalized
                    || tag
                        .strip_prefix(normalized)
                        .is_some_and(|rest| rest.starts_with('/'))
            })))
        }
        "inFolder" => {
            let folder = single_string_argument("file.inFolder", args, context)?;
            let folder = folder.trim().trim_matches('/');
            let candidate_folder = file_folder(&context.candidate.path);
            Ok(EvalValue::Bool(
                candidate_folder == folder || candidate_folder.starts_with(&format!("{folder}/")),
            ))
        }
        "hasLink" => {
            let needle = single_string_argument("file.hasLink", args, context)?;
            let needle = needle.trim();
            Ok(EvalValue::Bool(context.candidate.links.iter().any(
                |link| {
                    link == needle || file_name_without_extension(link).eq_ignore_ascii_case(needle)
                },
            )))
        }
        other => Err(BaseQueryError::UnsupportedExpression(format!(
            "unsupported file method '{other}'"
        ))),
    }
}

fn eval_value_method(
    target: EvalValue,
    method: &str,
    args: &[Expr],
    context: &EvalContext<'_>,
) -> Result<EvalValue, BaseQueryError> {
    match method {
        "contains" => {
            let [arg] = args else {
                return Err(BaseQueryError::InvalidExpression(
                    "contains() expects exactly one argument".to_string(),
                ));
            };
            let needle = eval_expr(arg, context)?;
            Ok(EvalValue::Bool(value_contains(&target, &needle)))
        }
        "isEmpty" => {
            if !args.is_empty() {
                return Err(BaseQueryError::InvalidExpression(
                    "isEmpty() expects no arguments".to_string(),
                ));
            }
            Ok(EvalValue::Bool(!target.truthy()))
        }
        other => Err(BaseQueryError::UnsupportedExpression(format!(
            "unsupported method '{other}'"
        ))),
    }
}

fn single_string_argument(
    function_name: &str,
    args: &[Expr],
    context: &EvalContext<'_>,
) -> Result<String, BaseQueryError> {
    let [arg] = args else {
        return Err(BaseQueryError::InvalidExpression(format!(
            "{function_name}() expects exactly one argument"
        )));
    };
    let value = eval_expr(arg, context)?;
    value_as_string(&value).ok_or_else(|| {
        BaseQueryError::InvalidExpression(format!("{function_name}() expects a string argument"))
    })
}

fn eval_binary(
    left: &Expr,
    operator: BinaryOp,
    right: &Expr,
    context: &EvalContext<'_>,
) -> Result<EvalValue, BaseQueryError> {
    match operator {
        BinaryOp::Or => {
            let left = eval_expr(left, context)?;
            if left.truthy() {
                return Ok(EvalValue::Bool(true));
            }
            Ok(EvalValue::Bool(eval_expr(right, context)?.truthy()))
        }
        BinaryOp::And => {
            let left = eval_expr(left, context)?;
            if !left.truthy() {
                return Ok(EvalValue::Bool(false));
            }
            Ok(EvalValue::Bool(eval_expr(right, context)?.truthy()))
        }
        BinaryOp::Eq | BinaryOp::Ne => {
            let left = eval_expr(left, context)?;
            let right = eval_expr(right, context)?;
            let equals = values_equal(&left, &right);
            Ok(EvalValue::Bool(if matches!(operator, BinaryOp::Eq) {
                equals
            } else {
                !equals
            }))
        }
        BinaryOp::Gt | BinaryOp::Gte | BinaryOp::Lt | BinaryOp::Lte => {
            let left = eval_expr(left, context)?;
            let right = eval_expr(right, context)?;
            let Some(ordering) = compare_values(&left, &right) else {
                return Ok(EvalValue::Bool(false));
            };
            let matched = match operator {
                BinaryOp::Gt => ordering == Ordering::Greater,
                BinaryOp::Gte => matches!(ordering, Ordering::Greater | Ordering::Equal),
                BinaryOp::Lt => ordering == Ordering::Less,
                BinaryOp::Lte => matches!(ordering, Ordering::Less | Ordering::Equal),
                _ => unreachable!(),
            };
            Ok(EvalValue::Bool(matched))
        }
    }
}

fn evaluate_property_id(
    property_id: &str,
    context: &EvalContext<'_>,
) -> Result<EvalValue, BaseQueryError> {
    if let Some(field) = property_id.strip_prefix("file.") {
        file_property(context.candidate, field)
    } else if let Some(field) = property_id.strip_prefix("note.") {
        Ok(frontmatter_path(&context.candidate.frontmatter, field))
    } else if property_id.starts_with("formula.") {
        Err(BaseQueryError::UnsupportedExpression(
            "formula properties are not supported yet".to_string(),
        ))
    } else {
        Ok(frontmatter_path(
            &context.candidate.frontmatter,
            property_id,
        ))
    }
}

fn file_property(candidate: &BaseQueryCandidate, field: &str) -> Result<EvalValue, BaseQueryError> {
    match field {
        "path" => Ok(EvalValue::String(candidate.path.clone())),
        "name" => Ok(EvalValue::String(file_name_without_extension(
            &candidate.path,
        ))),
        "folder" => Ok(EvalValue::String(file_folder(&candidate.path))),
        "ext" => Ok(EvalValue::String(file_extension(&candidate.path))),
        "tags" => Ok(EvalValue::Array(
            candidate
                .tags
                .iter()
                .cloned()
                .map(EvalValue::String)
                .collect(),
        )),
        "links" => Ok(EvalValue::Array(
            candidate
                .links
                .iter()
                .cloned()
                .map(EvalValue::String)
                .collect(),
        )),
        "ctime" => Ok(candidate
            .created_at
            .map(EvalValue::Date)
            .unwrap_or(EvalValue::Missing)),
        "mtime" => Ok(EvalValue::Date(candidate.updated_at)),
        "properties" => Ok(json_to_eval_value(&candidate.frontmatter)),
        other => Err(BaseQueryError::UnsupportedExpression(format!(
            "unsupported file property 'file.{other}'"
        ))),
    }
}

fn frontmatter_property(frontmatter: &JsonValue, field: &str) -> EvalValue {
    match frontmatter {
        JsonValue::Object(map) => map
            .get(field)
            .map(json_to_eval_value)
            .unwrap_or(EvalValue::Missing),
        _ => EvalValue::Missing,
    }
}

fn frontmatter_path(frontmatter: &JsonValue, path: &str) -> EvalValue {
    let mut current = frontmatter;
    for segment in path.split('.') {
        let JsonValue::Object(map) = current else {
            return EvalValue::Missing;
        };
        let Some(next) = map.get(segment) else {
            return EvalValue::Missing;
        };
        current = next;
    }
    json_to_eval_value(current)
}

fn json_to_eval_value(value: &JsonValue) -> EvalValue {
    match value {
        JsonValue::Null => EvalValue::Null,
        JsonValue::Bool(value) => EvalValue::Bool(*value),
        JsonValue::Number(value) => value
            .as_f64()
            .map(EvalValue::Number)
            .unwrap_or(EvalValue::Missing),
        JsonValue::String(value) => EvalValue::String(value.clone()),
        JsonValue::Array(values) => {
            EvalValue::Array(values.iter().map(json_to_eval_value).collect())
        }
        JsonValue::Object(values) => EvalValue::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_to_eval_value(value)))
                .collect(),
        ),
    }
}

fn values_equal(left: &EvalValue, right: &EvalValue) -> bool {
    match (left, right) {
        (EvalValue::Missing, EvalValue::Missing) | (EvalValue::Null, EvalValue::Null) => true,
        (EvalValue::Bool(left), EvalValue::Bool(right)) => left == right,
        (EvalValue::Number(left), EvalValue::Number(right)) => {
            (*left - *right).abs() < f64::EPSILON
        }
        (EvalValue::String(left), EvalValue::String(right)) => left == right,
        (EvalValue::Date(left), EvalValue::Date(right)) => left == right,
        (EvalValue::String(left), EvalValue::Date(right)) => {
            parse_date_value(left).is_some_and(|left| left == *right)
        }
        (EvalValue::Date(left), EvalValue::String(right)) => {
            parse_date_value(right).is_some_and(|right| *left == right)
        }
        (EvalValue::Array(left), EvalValue::Array(right)) => left == right,
        (EvalValue::Object(left), EvalValue::Object(right)) => left == right,
        _ => false,
    }
}

fn compare_values(left: &EvalValue, right: &EvalValue) -> Option<Ordering> {
    match (left, right) {
        (EvalValue::Number(left), EvalValue::Number(right)) => left.partial_cmp(right),
        (EvalValue::Date(left), EvalValue::Date(right)) => Some(left.cmp(right)),
        (EvalValue::String(left), EvalValue::Date(right)) => {
            parse_date_value(left).map(|left| left.cmp(right))
        }
        (EvalValue::Date(left), EvalValue::String(right)) => {
            parse_date_value(right).map(|right| left.cmp(&right))
        }
        (EvalValue::String(left), EvalValue::String(right)) => {
            if let (Some(left), Some(right)) = (parse_date_value(left), parse_date_value(right)) {
                Some(left.cmp(&right))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn value_contains(target: &EvalValue, needle: &EvalValue) -> bool {
    match target {
        EvalValue::Array(values) => values.iter().any(|value| values_equal(value, needle)),
        EvalValue::String(haystack) => value_as_string(needle)
            .as_deref()
            .is_some_and(|needle| haystack.contains(needle)),
        _ => false,
    }
}

fn value_as_string(value: &EvalValue) -> Option<String> {
    match value {
        EvalValue::String(value) => Some(value.clone()),
        EvalValue::Number(value) => Some(value.to_string()),
        EvalValue::Bool(value) => Some(value.to_string()),
        EvalValue::Date(value) => Some(value.to_rfc3339()),
        _ => None,
    }
}

fn parse_date_value(raw: &str) -> Option<DateTime<Utc>> {
    let raw = raw.trim();
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|value| value.and_utc())
        })
        .or_else(|| {
            NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                .ok()
                .and_then(|value| value.and_hms_opt(0, 0, 0))
                .map(|value| value.and_utc())
        })
}

fn compare_candidates(
    left: &BaseQueryCandidate,
    right: &BaseQueryCandidate,
    sort: &[SortSpec],
    now: DateTime<Utc>,
) -> Ordering {
    for spec in sort {
        let left_context = EvalContext {
            candidate: left,
            now,
        };
        let right_context = EvalContext {
            candidate: right,
            now,
        };
        let left_value = evaluate_property_id(&spec.property_id, &left_context).ok();
        let right_value = evaluate_property_id(&spec.property_id, &right_context).ok();
        let ordering = match (left_value.as_ref(), right_value.as_ref()) {
            (Some(left), Some(right)) => compare_values(left, right).unwrap_or_else(|| {
                left.clone()
                    .into_json()
                    .to_string()
                    .cmp(&right.clone().into_json().to_string())
            }),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        let ordering = match spec.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.path.cmp(&right.path)
}

fn infer_column_type(rows: &[BaseQueryRow], property_id: &str) -> BaseQueryValueType {
    let mut inferred = None;
    for row in rows {
        let Some(value) = row.cells.get(property_id) else {
            continue;
        };
        let Some(value_type) = json_value_type(value) else {
            continue;
        };
        inferred = Some(match inferred {
            None => value_type,
            Some(existing) if existing == value_type => existing,
            Some(_) => BaseQueryValueType::Mixed,
        });
        if inferred == Some(BaseQueryValueType::Mixed) {
            break;
        }
    }
    inferred.unwrap_or(BaseQueryValueType::Empty)
}

fn json_value_type(value: &JsonValue) -> Option<BaseQueryValueType> {
    match value {
        JsonValue::Null => None,
        JsonValue::Bool(_) => Some(BaseQueryValueType::Boolean),
        JsonValue::Number(_) => Some(BaseQueryValueType::Number),
        JsonValue::String(value) => Some(if parse_date_value(value).is_some() {
            BaseQueryValueType::Date
        } else {
            BaseQueryValueType::String
        }),
        JsonValue::Array(_) => Some(BaseQueryValueType::Array),
        JsonValue::Object(_) => Some(BaseQueryValueType::Object),
    }
}

fn default_property_label(property_id: &str) -> String {
    property_id
        .rsplit_once('.')
        .map(|(_, field)| field)
        .unwrap_or(property_id)
        .replace('_', " ")
}

fn file_name_without_extension(path: &str) -> String {
    let file_name = path.rsplit('/').next().unwrap_or(path);
    file_name
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(file_name)
        .to_string()
}

fn file_folder(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(folder, _)| folder.to_string())
        .unwrap_or_default()
}

fn file_extension(path: &str) -> String {
    path.rsplit_once('.')
        .map(|(_, extension)| extension.to_string())
        .unwrap_or_default()
}

fn value_kind(value: &EvalValue) -> &'static str {
    match value {
        EvalValue::Missing => "missing",
        EvalValue::Null => "null",
        EvalValue::Bool(_) => "boolean",
        EvalValue::Number(_) => "number",
        EvalValue::String(_) => "string",
        EvalValue::Date(_) => "date",
        EvalValue::Array(_) => "array",
        EvalValue::Object(_) => "object",
        EvalValue::File => "file",
        EvalValue::Note => "note",
        EvalValue::Formula => "formula",
    }
}

#[derive(Debug, Clone, PartialEq)]
enum TokenKind {
    Identifier(String),
    Number(f64),
    String(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
    Bang,
    AndAnd,
    OrOr,
    EqEq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
    End,
}

#[derive(Debug, Clone, PartialEq)]
struct Token {
    kind: TokenKind,
    offset: usize,
}

fn parse_expression(raw: &str) -> Result<Expr, BaseQueryError> {
    let tokens = Lexer::new(raw).tokenize()?;
    let mut parser = Parser {
        tokens,
        position: 0,
    };
    let expr = parser.parse_or()?;
    if !matches!(parser.peek().kind, TokenKind::End) {
        return Err(BaseQueryError::InvalidExpression(format!(
            "unexpected token at byte {}",
            parser.peek().offset
        )));
    }
    Ok(expr)
}

struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    fn parse_or(&mut self) -> Result<Expr, BaseQueryError> {
        let mut expr = self.parse_and()?;
        while self
            .consume(|kind| matches!(kind, TokenKind::OrOr))
            .is_some()
        {
            let right = self.parse_and()?;
            expr = Expr::Binary(Box::new(expr), BinaryOp::Or, Box::new(right));
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, BaseQueryError> {
        let mut expr = self.parse_equality()?;
        while self
            .consume(|kind| matches!(kind, TokenKind::AndAnd))
            .is_some()
        {
            let right = self.parse_equality()?;
            expr = Expr::Binary(Box::new(expr), BinaryOp::And, Box::new(right));
        }
        Ok(expr)
    }

    fn parse_equality(&mut self) -> Result<Expr, BaseQueryError> {
        let mut expr = self.parse_comparison()?;
        loop {
            let operator = if self
                .consume(|kind| matches!(kind, TokenKind::EqEq))
                .is_some()
            {
                Some(BinaryOp::Eq)
            } else if self
                .consume(|kind| matches!(kind, TokenKind::NotEq))
                .is_some()
            {
                Some(BinaryOp::Ne)
            } else {
                None
            };
            let Some(operator) = operator else {
                break;
            };
            let right = self.parse_comparison()?;
            expr = Expr::Binary(Box::new(expr), operator, Box::new(right));
        }
        Ok(expr)
    }

    fn parse_comparison(&mut self) -> Result<Expr, BaseQueryError> {
        let mut expr = self.parse_unary()?;
        loop {
            let operator = if self.consume(|kind| matches!(kind, TokenKind::Gt)).is_some() {
                Some(BinaryOp::Gt)
            } else if self
                .consume(|kind| matches!(kind, TokenKind::Gte))
                .is_some()
            {
                Some(BinaryOp::Gte)
            } else if self.consume(|kind| matches!(kind, TokenKind::Lt)).is_some() {
                Some(BinaryOp::Lt)
            } else if self
                .consume(|kind| matches!(kind, TokenKind::Lte))
                .is_some()
            {
                Some(BinaryOp::Lte)
            } else {
                None
            };
            let Some(operator) = operator else {
                break;
            };
            let right = self.parse_unary()?;
            expr = Expr::Binary(Box::new(expr), operator, Box::new(right));
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, BaseQueryError> {
        if self
            .consume(|kind| matches!(kind, TokenKind::Bang))
            .is_some()
        {
            return Ok(Expr::UnaryNot(Box::new(self.parse_unary()?)));
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr, BaseQueryError> {
        let mut expr = self.parse_primary()?;
        loop {
            if self
                .consume(|kind| matches!(kind, TokenKind::Dot))
                .is_some()
            {
                let field = match self.advance().kind.clone() {
                    TokenKind::Identifier(field) => field,
                    _ => {
                        return Err(BaseQueryError::InvalidExpression(
                            "expected identifier after '.'".to_string(),
                        ));
                    }
                };
                expr = Expr::Member(Box::new(expr), field);
                continue;
            }
            if self
                .consume(|kind| matches!(kind, TokenKind::LParen))
                .is_some()
            {
                let mut args = Vec::new();
                if !matches!(self.peek().kind, TokenKind::RParen) {
                    loop {
                        args.push(self.parse_or()?);
                        if self
                            .consume(|kind| matches!(kind, TokenKind::Comma))
                            .is_none()
                        {
                            break;
                        }
                    }
                }
                self.expect(|kind| matches!(kind, TokenKind::RParen), "expected ')'")?;
                expr = Expr::Call(Box::new(expr), args);
                continue;
            }
            if self
                .consume(|kind| matches!(kind, TokenKind::LBracket))
                .is_some()
            {
                let index = self.parse_or()?;
                self.expect(|kind| matches!(kind, TokenKind::RBracket), "expected ']'")?;
                expr = Expr::Index(Box::new(expr), Box::new(index));
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expr, BaseQueryError> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Identifier(name) if name == "true" => {
                Ok(Expr::Literal(EvalValue::Bool(true)))
            }
            TokenKind::Identifier(name) if name == "false" => {
                Ok(Expr::Literal(EvalValue::Bool(false)))
            }
            TokenKind::Identifier(name) if name == "null" => Ok(Expr::Literal(EvalValue::Null)),
            TokenKind::Identifier(name) => Ok(Expr::Identifier(name)),
            TokenKind::Number(value) => Ok(Expr::Literal(EvalValue::Number(value))),
            TokenKind::String(value) => Ok(Expr::Literal(EvalValue::String(value))),
            TokenKind::LParen => {
                let expr = self.parse_or()?;
                self.expect(|kind| matches!(kind, TokenKind::RParen), "expected ')'")?;
                Ok(expr)
            }
            other => Err(BaseQueryError::InvalidExpression(format!(
                "unexpected token {:?} at byte {}",
                other, token.offset
            ))),
        }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.position]
    }

    fn advance(&mut self) -> &Token {
        let token = &self.tokens[self.position];
        self.position += 1;
        token
    }

    fn consume(&mut self, predicate: impl FnOnce(&TokenKind) -> bool) -> Option<Token> {
        if predicate(&self.peek().kind) {
            Some(self.advance().clone())
        } else {
            None
        }
    }

    fn expect(
        &mut self,
        predicate: impl FnOnce(&TokenKind) -> bool,
        message: &str,
    ) -> Result<(), BaseQueryError> {
        if predicate(&self.peek().kind) {
            self.advance();
            Ok(())
        } else {
            Err(BaseQueryError::InvalidExpression(format!(
                "{message} at byte {}",
                self.peek().offset
            )))
        }
    }
}

struct Lexer<'a> {
    input: &'a str,
    cursor: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, cursor: 0 }
    }

    fn tokenize(mut self) -> Result<Vec<Token>, BaseQueryError> {
        let mut tokens = Vec::new();
        while let Some((offset, ch)) = self.peek_char() {
            if ch.is_whitespace() {
                self.bump_char();
                continue;
            }

            let kind = match ch {
                '(' => {
                    self.bump_char();
                    TokenKind::LParen
                }
                ')' => {
                    self.bump_char();
                    TokenKind::RParen
                }
                '[' => {
                    self.bump_char();
                    TokenKind::LBracket
                }
                ']' => {
                    self.bump_char();
                    TokenKind::RBracket
                }
                ',' => {
                    self.bump_char();
                    TokenKind::Comma
                }
                '.' => {
                    self.bump_char();
                    TokenKind::Dot
                }
                '!' => {
                    self.bump_char();
                    if self.consume_char('=') {
                        TokenKind::NotEq
                    } else {
                        TokenKind::Bang
                    }
                }
                '=' => {
                    self.bump_char();
                    if self.consume_char('=') {
                        TokenKind::EqEq
                    } else {
                        return Err(BaseQueryError::InvalidExpression(format!(
                            "expected '=' after '=' at byte {offset}"
                        )));
                    }
                }
                '>' => {
                    self.bump_char();
                    if self.consume_char('=') {
                        TokenKind::Gte
                    } else {
                        TokenKind::Gt
                    }
                }
                '<' => {
                    self.bump_char();
                    if self.consume_char('=') {
                        TokenKind::Lte
                    } else {
                        TokenKind::Lt
                    }
                }
                '&' => {
                    self.bump_char();
                    if self.consume_char('&') {
                        TokenKind::AndAnd
                    } else {
                        return Err(BaseQueryError::InvalidExpression(format!(
                            "expected '&' after '&' at byte {offset}"
                        )));
                    }
                }
                '|' => {
                    self.bump_char();
                    if self.consume_char('|') {
                        TokenKind::OrOr
                    } else {
                        return Err(BaseQueryError::InvalidExpression(format!(
                            "expected '|' after '|' at byte {offset}"
                        )));
                    }
                }
                '\'' | '"' => TokenKind::String(self.lex_string(ch)?),
                '-' | '0'..='9' => TokenKind::Number(self.lex_number()?),
                _ if is_ident_start(ch) => TokenKind::Identifier(self.lex_identifier()),
                _ => {
                    return Err(BaseQueryError::InvalidExpression(format!(
                        "unexpected character '{ch}' at byte {offset}"
                    )));
                }
            };
            tokens.push(Token { kind, offset });
        }
        tokens.push(Token {
            kind: TokenKind::End,
            offset: self.input.len(),
        });
        Ok(tokens)
    }

    fn peek_char(&self) -> Option<(usize, char)> {
        self.input[self.cursor..]
            .char_indices()
            .next()
            .map(|(offset, ch)| (self.cursor + offset, ch))
    }

    fn bump_char(&mut self) -> Option<char> {
        let (_, ch) = self.peek_char()?;
        self.cursor += ch.len_utf8();
        Some(ch)
    }

    fn consume_char(&mut self, expected: char) -> bool {
        if self.peek_char().is_some_and(|(_, ch)| ch == expected) {
            self.bump_char();
            true
        } else {
            false
        }
    }

    fn lex_identifier(&mut self) -> String {
        let start = self.cursor;
        while self
            .peek_char()
            .is_some_and(|(_, ch)| is_ident_continue(ch))
        {
            self.bump_char();
        }
        self.input[start..self.cursor].to_string()
    }

    fn lex_number(&mut self) -> Result<f64, BaseQueryError> {
        let start = self.cursor;
        if self.peek_char().is_some_and(|(_, ch)| ch == '-') {
            self.bump_char();
        }
        while self.peek_char().is_some_and(|(_, ch)| ch.is_ascii_digit()) {
            self.bump_char();
        }
        if self.peek_char().is_some_and(|(_, ch)| ch == '.') {
            self.bump_char();
            while self.peek_char().is_some_and(|(_, ch)| ch.is_ascii_digit()) {
                self.bump_char();
            }
        }
        self.input[start..self.cursor].parse::<f64>().map_err(|_| {
            BaseQueryError::InvalidExpression(format!("invalid number at byte {start}"))
        })
    }

    fn lex_string(&mut self, quote: char) -> Result<String, BaseQueryError> {
        let start = self.cursor;
        self.bump_char();
        let mut value = String::new();
        while let Some((_, ch)) = self.peek_char() {
            self.bump_char();
            if ch == quote {
                return Ok(value);
            }
            if ch == '\\' {
                let Some((_, escaped)) = self.peek_char() else {
                    break;
                };
                self.bump_char();
                value.push(match escaped {
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    '\\' => '\\',
                    '\'' => '\'',
                    '"' => '"',
                    other => other,
                });
            } else {
                value.push(ch);
            }
        }
        Err(BaseQueryError::InvalidExpression(format!(
            "unterminated string at byte {start}"
        )))
    }
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn candidate(path: &str, frontmatter: JsonValue) -> BaseQueryCandidate {
        BaseQueryCandidate {
            id: NoteId::new(path),
            path: path.to_string(),
            title: file_name_without_extension(path),
            frontmatter,
            tags: vec!["workout-exercise".to_string()],
            created_at: Some(
                DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                    .expect("date")
                    .with_timezone(&Utc),
            ),
            updated_at: DateTime::parse_from_rfc3339("2026-07-01T00:00:00Z")
                .expect("date")
                .with_timezone(&Utc),
            links: vec!["11Active/Workout exercise pool 2026.md".to_string()],
        }
    }

    #[test]
    fn query_base_filters_and_projects_structured_rows() {
        let request = QueryBaseRequest {
            base_query: r#"
filters:
  and:
    - file.inFolder("11Active/WorkoutExercises")
    - file.hasTag("workout-exercise")
    - list(exercise_status).contains("unassessed")
views:
  - type: table
    name: Exercises
    order:
      - file.name
      - note.equipment
      - note.exercise_status
    limit: 10
properties:
  note.exercise_status:
    displayName: Status
"#
            .to_string(),
            view: Some("Exercises".to_string()),
            limit: None,
        };
        let response = execute_query_base(
            request,
            vec![
                candidate(
                    "11Active/WorkoutExercises/Ring support hold.md",
                    json!({
                        "equipment": ["rings"],
                        "exercise_status": ["unassessed"]
                    }),
                ),
                candidate(
                    "11Active/Other.md",
                    json!({"equipment": ["rings"], "exercise_status": ["unassessed"]}),
                ),
            ],
            500,
            Utc::now(),
        )
        .expect("query succeeds");

        assert_eq!(response.total, 1);
        assert_eq!(response.returned, 1);
        assert_eq!(response.columns[2].id, "note.exercise_status");
        assert_eq!(response.columns[2].label, "Status");
        assert_eq!(response.rows[0].cells["note.equipment"], json!(["rings"]));
    }

    #[test]
    fn query_base_supports_scalar_bool_number_date_and_missing_comparisons() {
        let request = QueryBaseRequest {
            base_query: r#"
filters:
  and:
    - status == "active"
    - watchlist == false
    - load_kg >= 24
    - missing_field != "blocked"
    - file.mtime >= date("2026-06-01")
views:
  - type: table
    order:
      - file.path
      - status
      - load_kg
"#
            .to_string(),
            view: None,
            limit: None,
        };
        let response = execute_query_base(
            request,
            vec![candidate(
                "11Active/WorkoutExercises/Kettlebell swing.md",
                json!({"status": "active", "watchlist": false, "load_kg": 24}),
            )],
            500,
            Utc::now(),
        )
        .expect("query succeeds");

        assert_eq!(response.total, 1);
        assert_eq!(response.columns[1].value_type, BaseQueryValueType::String);
        assert_eq!(response.columns[2].value_type, BaseQueryValueType::Number);
    }

    #[test]
    fn query_base_rejects_unsupported_formulas() {
        let request = QueryBaseRequest {
            base_query: r#"
views:
  - type: table
    order:
      - formula.ppu
"#
            .to_string(),
            view: None,
            limit: None,
        };
        let error = execute_query_base(
            request,
            vec![candidate("notes/a.md", json!({"price": 10}))],
            500,
            Utc::now(),
        )
        .expect_err("formulas are unsupported");

        assert!(error.to_string().contains("formula"));
    }
}
