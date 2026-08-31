use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug)]
pub struct SpecSource<'a> {
    pub name: &'a str,
    pub contents: &'a str,
}

#[derive(Debug)]
pub(crate) struct SpecDocument {
    pub(crate) requirements: BTreeSet<String>,
}

pub(crate) fn parse(sources: &[SpecSource<'_>]) -> Result<SpecDocument, Vec<String>> {
    let mut declarations = BTreeMap::new();
    let mut errors = Vec::new();

    for source in sources {
        let mut source_count = 0;
        for (line_index, line) in source.contents.lines().enumerate() {
            let Some(id) = declared_id(line) else {
                continue;
            };
            source_count += 1;
            let location = format!("{}:{}", source.name, line_index + 1);
            if let Some(previous) = declarations.insert(id.to_owned(), location.clone()) {
                errors.push(format!(
                    "SPEC ID `{id}` is declared more than once: {previous} and {location}"
                ));
            }
        }
        if source_count == 0 {
            errors.push(format!(
                "SPEC document `{}` contains no requirement/state declarations",
                source.name
            ));
        }
    }

    if sources.is_empty() {
        errors.push("no SPEC documents were provided".to_owned());
    }

    if errors.is_empty() {
        Ok(SpecDocument {
            requirements: declarations.into_keys().collect(),
        })
    } else {
        Err(errors)
    }
}

fn declared_id(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    let candidate = if let Some(rest) = trimmed.strip_prefix("- **`") {
        rest.split_once('`').map(|(id, _)| id)
    } else if let Some(rest) = trimmed.strip_prefix("| `") {
        rest.split_once('`').map(|(id, _)| id)
    } else {
        None
    }?;

    is_requirement_id(candidate).then_some(candidate)
}

fn is_requirement_id(value: &str) -> bool {
    let Some((prefix, sequence)) = value.rsplit_once('-') else {
        return false;
    };
    sequence.len() == 3
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && !prefix.is_empty()
        && prefix.split('-').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        })
}
