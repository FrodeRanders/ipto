use std::collections::{HashMap, HashSet};

use serde::Serialize;
use serde_json::{json, Value};

use crate::backend::{RepoError, RepoResult};

// Lightweight SDL parser used for configuration/setup flows.
// It intentionally extracts only the subset we need:
// - attribute registry entries
// - record/template declarations and @use mappings
// It does not attempt full GraphQL validation/execution semantics.

#[derive(Debug, Clone, Serialize)]
pub struct SdlAttributeSpec {
    pub alias: String,
    pub name: String,
    pub qualname: String,
    pub attribute_type: String,
    pub is_array: bool,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SdlRecordFieldSpec {
    pub field_name: String,
    pub attribute_alias: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SdlRecordSpec {
    pub type_name: String,
    pub attribute_alias: String,
    pub fields: Vec<SdlRecordFieldSpec>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SdlTemplateSpec {
    pub type_name: String,
    pub template_name: String,
    pub fields: Vec<SdlRecordFieldSpec>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SdlCatalog {
    pub attributes: Vec<SdlAttributeSpec>,
    pub records: Vec<SdlRecordSpec>,
    pub templates: Vec<SdlTemplateSpec>,
}

pub fn parse_graphql_sdl(sdl: &str) -> RepoResult<SdlCatalog> {
    // Strip line comments first to keep block scanning deterministic.
    let cleaned = strip_hash_comments(sdl);
    let enum_blocks = extract_blocks(&cleaned, "enum");
    let type_blocks = extract_blocks(&cleaned, "type");

    let mut attributes = Vec::new();
    for (header, body) in enum_blocks {
        if !header.contains("@attributeRegistry") {
            continue;
        }
        attributes.extend(parse_attribute_registry_body(&body)?);
    }

    let mut records = Vec::new();
    let mut templates = Vec::new();
    for (header, body) in type_blocks {
        let type_name = parse_type_name(&header)?;
        if let Some(record_args) = extract_directive_args(&header, "record") {
            let args = parse_argument_map(record_args);
            let attribute_alias = args
                .get("attribute")
                .cloned()
                .unwrap_or_else(|| type_name.clone());
            records.push(SdlRecordSpec {
                type_name: type_name.clone(),
                attribute_alias,
                fields: parse_use_fields(&body),
            });
        }
        if let Some(template_args) = extract_directive_args(&header, "template") {
            let args = parse_argument_map(template_args);
            let template_name = args
                .get("name")
                .cloned()
                .unwrap_or_else(|| type_name.clone());
            templates.push(SdlTemplateSpec {
                type_name: type_name.clone(),
                template_name,
                fields: parse_use_fields(&body),
            });
        }
    }

    Ok(SdlCatalog {
        attributes,
        records,
        templates,
    })
}

pub fn catalog_as_json(catalog: &SdlCatalog) -> Value {
    json!({
        "attributes": catalog.attributes,
        "records": catalog.records,
        "templates": catalog.templates
    })
}

pub fn normalize_attribute_type(input: &str) -> String {
    if input.trim().parse::<i32>().is_ok() {
        return input.trim().to_string();
    }
    match input.trim().to_ascii_lowercase().as_str() {
        "string" => "string".to_string(),
        "time" | "instant" | "timestamp" | "datetime" => "time".to_string(),
        "int" | "integer" => "int".to_string(),
        "long" => "long".to_string(),
        "double" | "float" => "double".to_string(),
        "bool" | "boolean" => "bool".to_string(),
        "data" | "bytes" | "blob" => "data".to_string(),
        "record" => "record".to_string(),
        other => other.to_string(),
    }
}

pub fn attribute_type_is_record(input: &str) -> bool {
    let normalized = normalize_attribute_type(input);
    normalized == "record" || normalized == "99"
}

pub fn validate_catalog_references(catalog: &SdlCatalog) -> RepoResult<()> {
    let known_aliases: HashSet<&str> = catalog
        .attributes
        .iter()
        .map(|a| a.alias.as_str())
        .collect();

    for record in &catalog.records {
        if !known_aliases.contains(record.attribute_alias.as_str()) {
            return Err(RepoError::InvalidInput(format!(
                "record '{}' references unknown record attribute alias '{}'",
                record.type_name, record.attribute_alias
            )));
        }
        for field in &record.fields {
            if !known_aliases.contains(field.attribute_alias.as_str()) {
                return Err(RepoError::InvalidInput(format!(
                    "record '{}.{}' references unknown attribute alias '{}'",
                    record.type_name, field.field_name, field.attribute_alias
                )));
            }
        }
    }

    for template in &catalog.templates {
        for field in &template.fields {
            if !known_aliases.contains(field.attribute_alias.as_str()) {
                return Err(RepoError::InvalidInput(format!(
                    "template '{}.{}' references unknown attribute alias '{}'",
                    template.type_name, field.field_name, field.attribute_alias
                )));
            }
        }
    }
    Ok(())
}

fn parse_attribute_registry_body(body: &str) -> RepoResult<Vec<SdlAttributeSpec>> {
    let mut attributes = Vec::new();
    let mut i = 0usize;
    let bytes = body.as_bytes();

    while i < bytes.len() {
        skip_ws_and_strings(body, &mut i);
        if i >= bytes.len() {
            break;
        }

        let Some(alias) = parse_identifier(body, &mut i) else {
            i += 1;
            continue;
        };

        skip_ws(body, &mut i);
        if !body[i..].starts_with("@attribute") {
            continue;
        }
        i += "@attribute".len();
        skip_ws(body, &mut i);

        let args_text = if i < bytes.len() && bytes[i] == b'(' {
            let end = find_matching_paren(body, i).ok_or_else(|| {
                RepoError::InvalidInput(format!(
                    "malformed @attribute directive for alias '{alias}': missing ')'"
                ))
            })?;
            let content = body[i + 1..end].to_string();
            i = end + 1;
            content
        } else {
            String::new()
        };

        let args = parse_argument_map(&args_text);
        let datatype = args
            .get("datatype")
            .cloned()
            .unwrap_or_else(|| "RECORD".to_string());
        // Match Java behavior: absent datatype defaults to RECORD.
        let attribute_type = normalize_attribute_type(&datatype);
        let is_array = args
            .get("array")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let name = args.get("name").cloned().unwrap_or_else(|| alias.clone());
        let qualname = args
            .get("uri")
            .cloned()
            .or_else(|| args.get("qualname").cloned())
            .unwrap_or_else(|| name.clone());
        let description = args.get("description").cloned();

        attributes.push(SdlAttributeSpec {
            alias,
            name,
            qualname,
            attribute_type,
            is_array,
            description,
        });
    }

    Ok(attributes)
}

fn parse_use_fields(body: &str) -> Vec<SdlRecordFieldSpec> {
    body.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('"') {
                return None;
            }
            let use_args = extract_directive_args(trimmed, "use")?;
            let args = parse_argument_map(use_args);
            let attribute_alias = args.get("attribute")?.clone();
            let field_name = parse_field_name(trimmed)?;
            Some(SdlRecordFieldSpec {
                field_name,
                attribute_alias,
            })
        })
        .collect()
}

fn parse_type_name(header: &str) -> RepoResult<String> {
    let mut i = 0usize;
    parse_identifier(header, &mut i);
    skip_ws(header, &mut i);
    parse_identifier(header, &mut i).ok_or_else(|| {
        RepoError::InvalidInput(format!(
            "malformed SDL type declaration header: '{}'",
            header.trim()
        ))
    })
}

fn parse_field_name(line: &str) -> Option<String> {
    let mut i = 0usize;
    parse_identifier(line, &mut i)
}

fn strip_hash_comments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_string = false;
    let mut in_block_string = false;
    let mut escaped = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_block_string {
            if ch == '"' && chars.peek() == Some(&'"') {
                let mut lookahead = chars.clone();
                lookahead.next();
                if lookahead.peek() == Some(&'"') {
                    out.push('"');
                    out.push(chars.next().unwrap_or('"'));
                    out.push(chars.next().unwrap_or('"'));
                    in_block_string = false;
                    continue;
                }
            }
            out.push(ch);
            continue;
        }

        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' && chars.peek() == Some(&'"') {
            let mut lookahead = chars.clone();
            lookahead.next();
            if lookahead.peek() == Some(&'"') {
                out.push('"');
                out.push(chars.next().unwrap_or('"'));
                out.push(chars.next().unwrap_or('"'));
                in_block_string = true;
                continue;
            }
            in_string = true;
            out.push(ch);
            continue;
        }

        if ch == '"' {
            in_string = true;
            out.push(ch);
            continue;
        }

        if ch == '#' {
            for next in chars.by_ref() {
                if next == '\n' {
                    out.push('\n');
                    break;
                }
            }
            continue;
        }

        out.push(ch);
    }

    out
}

fn extract_blocks(source: &str, keyword: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut cursor = 0usize;

    while let Some(start) = find_keyword(source, keyword, cursor) {
        // We locate matching braces outside quoted strings, so embedded JSON-like
        // text in SDL descriptions does not break block extraction.
        let Some(open_brace) = find_char_outside_strings(source, '{', start + keyword.len()) else {
            break;
        };
        let Some(close_brace) = find_matching_brace(source, open_brace) else {
            break;
        };

        let header = source[start..open_brace].to_string();
        let body = source[open_brace + 1..close_brace].to_string();
        out.push((header, body));
        cursor = close_brace + 1;
    }

    out
}

fn find_keyword(source: &str, keyword: &str, start: usize) -> Option<usize> {
    let mut cursor = start;
    while let Some(offset) = source[cursor..].find(keyword) {
        let pos = cursor + offset;
        let before_ok = pos == 0 || !source[..pos].chars().next_back().is_some_and(is_ident_char);
        let after = pos + keyword.len();
        let after_ok =
            after >= source.len() || !source[after..].chars().next().is_some_and(is_ident_char);
        if before_ok && after_ok {
            return Some(pos);
        }
        cursor = pos + keyword.len();
    }
    None
}

fn extract_directive_args<'a>(source: &'a str, directive_name: &str) -> Option<&'a str> {
    let marker = format!("@{directive_name}");
    let pos = source.find(&marker)?;
    let mut i = pos + marker.len();
    skip_ws(source, &mut i);
    if i >= source.len() || source.as_bytes()[i] != b'(' {
        return Some("");
    }
    let end = find_matching_paren(source, i)?;
    Some(&source[i + 1..end])
}

fn parse_argument_map(input: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for part in split_top_level(input, ',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((raw_key, raw_value)) = trimmed.split_once(':') else {
            continue;
        };
        let key = raw_key.trim().to_string();
        let value = parse_scalar_value(raw_value.trim());
        if !key.is_empty() {
            out.insert(key, value);
        }
    }
    out
}

fn parse_scalar_value(raw: &str) -> String {
    if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
        return unescape_quoted(&raw[1..raw.len() - 1]);
    }
    raw.trim_matches(',').trim().to_string()
}

fn unescape_quoted(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                match next {
                    '\\' => out.push('\\'),
                    '"' => out.push('"'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    other => {
                        out.push('\\');
                        out.push(other);
                    }
                }
            } else {
                out.push('\\');
            }
            continue;
        }
        out.push(ch);
    }
    out
}

fn split_top_level(input: &str, separator: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' => paren_depth += 1,
            ')' => paren_depth -= 1,
            _ if ch == separator && paren_depth == 0 => {
                out.push(&input[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&input[start..]);
    out
}

fn find_char_outside_strings(source: &str, needle: char, start: usize) -> Option<usize> {
    let mut in_string = false;
    let mut escaped = false;
    let mut in_block_string = false;
    let chars: Vec<(usize, char)> = source.char_indices().collect();
    let mut idx = 0usize;

    while idx < chars.len() {
        let (pos, ch) = chars[idx];
        if pos < start {
            idx += 1;
            continue;
        }

        if in_block_string {
            if ch == '"'
                && idx + 2 < chars.len()
                && chars[idx + 1].1 == '"'
                && chars[idx + 2].1 == '"'
            {
                in_block_string = false;
                idx += 3;
                continue;
            }
            idx += 1;
            continue;
        }

        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            idx += 1;
            continue;
        }

        if ch == '"' && idx + 2 < chars.len() && chars[idx + 1].1 == '"' && chars[idx + 2].1 == '"'
        {
            in_block_string = true;
            idx += 3;
            continue;
        }
        if ch == '"' {
            in_string = true;
            idx += 1;
            continue;
        }
        if ch == needle {
            return Some(pos);
        }
        idx += 1;
    }

    None
}

fn find_matching_brace(source: &str, open_idx: usize) -> Option<usize> {
    find_matching_enclosure(source, open_idx, '{', '}')
}

fn find_matching_paren(source: &str, open_idx: usize) -> Option<usize> {
    find_matching_enclosure(source, open_idx, '(', ')')
}

fn find_matching_enclosure(
    source: &str,
    open_idx: usize,
    open: char,
    close: char,
) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_string = false;
    let mut in_block_string = false;
    let mut escaped = false;
    let chars: Vec<(usize, char)> = source.char_indices().collect();
    let mut idx = 0usize;

    while idx < chars.len() {
        let (pos, ch) = chars[idx];
        if pos < open_idx {
            idx += 1;
            continue;
        }

        if in_block_string {
            if ch == '"'
                && idx + 2 < chars.len()
                && chars[idx + 1].1 == '"'
                && chars[idx + 2].1 == '"'
            {
                in_block_string = false;
                idx += 3;
                continue;
            }
            idx += 1;
            continue;
        }

        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            idx += 1;
            continue;
        }

        if ch == '"' && idx + 2 < chars.len() && chars[idx + 1].1 == '"' && chars[idx + 2].1 == '"'
        {
            in_block_string = true;
            idx += 3;
            continue;
        }
        if ch == '"' {
            in_string = true;
            idx += 1;
            continue;
        }

        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                return Some(pos);
            }
        }
        idx += 1;
    }

    None
}

fn parse_identifier(source: &str, i: &mut usize) -> Option<String> {
    skip_ws(source, i);
    if *i >= source.len() {
        return None;
    }
    let first = source[*i..].chars().next()?;
    if !is_ident_start(first) {
        return None;
    }
    let mut end = *i + first.len_utf8();
    for ch in source[end..].chars() {
        if !is_ident_char(ch) {
            break;
        }
        end += ch.len_utf8();
    }
    let token = source[*i..end].to_string();
    *i = end;
    Some(token)
}

fn skip_ws(source: &str, i: &mut usize) {
    while *i < source.len() {
        let ch = source[*i..].chars().next().unwrap_or('\0');
        if ch.is_whitespace() {
            *i += ch.len_utf8();
            continue;
        }
        break;
    }
}

fn skip_ws_and_strings(source: &str, i: &mut usize) {
    loop {
        skip_ws(source, i);
        if *i >= source.len() {
            return;
        }

        if source[*i..].starts_with("\"\"\"") {
            *i += 3;
            while *i < source.len() && !source[*i..].starts_with("\"\"\"") {
                let ch = source[*i..].chars().next().unwrap_or('\0');
                *i += ch.len_utf8();
            }
            if *i < source.len() {
                *i += 3;
            }
            continue;
        }

        let ch = source[*i..].chars().next().unwrap_or('\0');
        if ch == '"' {
            *i += 1;
            let mut escaped = false;
            while *i < source.len() {
                let next = source[*i..].chars().next().unwrap_or('\0');
                *i += next.len_utf8();
                if escaped {
                    escaped = false;
                    continue;
                }
                if next == '\\' {
                    escaped = true;
                    continue;
                }
                if next == '"' {
                    break;
                }
            }
            continue;
        }
        return;
    }
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_ident_char(ch: char) -> bool {
    is_ident_start(ch) || ch.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::{
        attribute_type_is_record, normalize_attribute_type, parse_graphql_sdl,
        validate_catalog_references,
    };

    #[test]
    fn parses_attribute_registry_and_record_template_shapes() {
        let sdl = r#"
        enum Attributes @attributeRegistry {
            id @attribute(datatype: STRING, array: false, name: "domain:id", uri: "urn:domain:id")
            address @attribute(datatype: RECORD, name: "domain:address", uri: "urn:domain:address")
            street @attribute(datatype: STRING, name: "domain:street")
            zip @attribute(datatype: INTEGER, array: false, name: "domain:zip")
        }

        type Address @record(attribute: address) {
            street: String @use(attribute: street)
            zip: Int @use(attribute: zip)
        }

        type Citizen @template(name: PersonTemplate) {
            id: String @use(attribute: id)
            address: Address @use(attribute: address)
        }
        "#;

        let catalog = parse_graphql_sdl(sdl).expect("SDL should parse");
        assert_eq!(catalog.attributes.len(), 4);
        assert_eq!(catalog.records.len(), 1);
        assert_eq!(catalog.templates.len(), 1);
        assert_eq!(catalog.records[0].attribute_alias, "address");
        assert_eq!(catalog.records[0].fields.len(), 2);
        assert_eq!(catalog.templates[0].template_name, "PersonTemplate");
    }

    #[test]
    fn validates_references_between_records_templates_and_attributes() {
        let sdl = r#"
        enum Attributes @attributeRegistry {
            address @attribute(datatype: RECORD)
            street @attribute(datatype: STRING)
        }
        type Address @record(attribute: address) {
            street: String @use(attribute: street)
        }
        "#;
        let catalog = parse_graphql_sdl(sdl).expect("SDL should parse");
        validate_catalog_references(&catalog).expect("references should be valid");
    }

    #[test]
    fn normalize_attribute_type_handles_common_graphql_datatypes() {
        assert_eq!(normalize_attribute_type("STRING"), "string");
        assert_eq!(normalize_attribute_type("TIME"), "time");
        assert_eq!(normalize_attribute_type("INTEGER"), "int");
        assert_eq!(normalize_attribute_type("DOUBLE"), "double");
        assert_eq!(normalize_attribute_type("BOOLEAN"), "bool");
        assert_eq!(normalize_attribute_type("DATA"), "data");
        assert_eq!(normalize_attribute_type("RECORD"), "record");
        assert!(attribute_type_is_record("RECORD"));
        assert!(attribute_type_is_record("99"));
    }
}
