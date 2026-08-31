use std::collections::BTreeSet;

#[derive(Debug)]
pub(crate) struct MatrixDocument {
    pub(crate) cases: BTreeSet<String>,
    pub(crate) requirements: BTreeSet<String>,
}

pub(crate) fn parse(source: &str) -> Result<MatrixDocument, Vec<String>> {
    let mut cases = BTreeSet::new();
    let mut requirements = BTreeSet::new();
    let mut errors = Vec::new();
    let mut requirement_column = false;

    for (line_index, line) in source.lines().enumerate() {
        let line_number = line_index + 1;
        let Some(cells) = markdown_cells(line) else {
            continue;
        };
        if cells.len() < 2 {
            continue;
        }

        if cells[0] == "Test ID" {
            requirement_column = cells[1] == "Requirement";
            continue;
        }

        let test_ids: Vec<_> = code_spans(cells[0])
            .into_iter()
            .filter(|value| value.starts_with("T-"))
            .collect();
        if test_ids.is_empty() {
            continue;
        }
        if test_ids.len() != 1 {
            errors.push(format!(
                "matrix line {line_number}: expected exactly one T-* ID, found {}",
                test_ids.len()
            ));
            continue;
        }

        let test_id = test_ids[0];
        if !is_test_id(test_id) {
            errors.push(format!(
                "matrix line {line_number}: invalid test case ID `{test_id}`"
            ));
            continue;
        }

        let linked_requirements: BTreeSet<_> = code_spans(cells[1])
            .into_iter()
            .filter(|value| is_requirement_id(value))
            .map(str::to_owned)
            .collect();
        if requirement_column && linked_requirements.is_empty() {
            errors.push(format!(
                "matrix line {line_number}: `{test_id}` has no linked SPEC requirement/state ID"
            ));
        }

        requirements.extend(linked_requirements.iter().cloned());
        if !cases.insert(test_id.to_owned()) {
            errors.push(format!(
                "matrix line {line_number}: duplicate test case ID `{test_id}`"
            ));
        }
    }

    if cases.is_empty() {
        errors.push("matrix contains no T-* test case rows".to_owned());
    }
    if requirements.is_empty() {
        errors.push("matrix contains no linked SPEC requirement/state IDs".to_owned());
    }

    if errors.is_empty() {
        Ok(MatrixDocument {
            cases,
            requirements,
        })
    } else {
        Err(errors)
    }
}

fn markdown_cells(line: &str) -> Option<Vec<&str>> {
    let trimmed = line.trim();
    let body = trimmed.strip_prefix('|')?;
    let body = body.strip_suffix('|').unwrap_or(body);
    Some(body.split('|').map(str::trim).collect())
}

fn code_spans(value: &str) -> Vec<&str> {
    let mut spans = Vec::new();
    let mut remainder = value;
    while let Some(open) = remainder.find('`') {
        remainder = &remainder[open + 1..];
        let Some(close) = remainder.find('`') else {
            break;
        };
        spans.push(&remainder[..close]);
        remainder = &remainder[close + 1..];
    }
    spans
}

fn is_test_id(value: &str) -> bool {
    value
        .strip_prefix("T-")
        .is_some_and(valid_uppercase_segments)
}

fn is_requirement_id(value: &str) -> bool {
    let Some((prefix, sequence)) = value.rsplit_once('-') else {
        return false;
    };
    sequence.len() == 3
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && valid_uppercase_segments(prefix)
}

fn valid_uppercase_segments(value: &str) -> bool {
    !value.is_empty()
        && value.split('-').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        })
}
