use crate::coverage::{self, CoverageCase, CoverageDocument, CoverageStatus};
use crate::matrix::{self, MatrixDocument};
use crate::spec::{self, SpecDocument, SpecSource};
use crate::test_list::{self, TestList};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Check,
    Gate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusDetail {
    pub id: String,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Report {
    pub matrix_cases: usize,
    pub declared_spec_ids: usize,
    pub linked_requirements: usize,
    pub listed_rust_tests: usize,
    pub executable: usize,
    pub partial: Vec<StatusDetail>,
    pub gap: Vec<StatusDetail>,
    pub out_of_scope: Vec<StatusDetail>,
}

pub fn validate(
    mode: Mode,
    matrix_source: &str,
    coverage_source: &str,
    test_list_source: &str,
    spec_sources: &[SpecSource<'_>],
) -> Result<Report, Vec<String>> {
    let matrix = matrix::parse(matrix_source);
    let coverage = coverage::parse(coverage_source);
    let test_list = test_list::parse(test_list_source);
    let spec = spec::parse(spec_sources);

    let mut errors = Vec::new();
    let matrix = collect_result(matrix, &mut errors);
    let coverage = collect_result(coverage, &mut errors);
    let test_list = collect_result(test_list, &mut errors);
    let spec = collect_result(spec, &mut errors);
    let (Some(matrix), Some(coverage), Some(test_list), Some(spec)) =
        (matrix, coverage, test_list, spec)
    else {
        return Err(errors);
    };

    validate_requirement_links(&matrix, &spec, &mut errors);
    let report = validate_coverage(mode, &matrix, &spec, &coverage, &test_list, &mut errors);

    if errors.is_empty() {
        Ok(report)
    } else {
        Err(errors)
    }
}

fn validate_requirement_links(
    matrix: &MatrixDocument,
    spec: &SpecDocument,
    errors: &mut Vec<String>,
) {
    for id in &spec.requirements {
        if !matrix.requirements.contains(id) {
            errors.push(format!(
                "SPEC requirement/state ID `{id}` is not linked by any matrix case"
            ));
        }
    }
    for id in &matrix.requirements {
        if !spec.requirements.contains(id) {
            errors.push(format!(
                "matrix links unknown SPEC requirement/state ID `{id}`"
            ));
        }
    }
}

fn collect_result<T>(result: Result<T, Vec<String>>, errors: &mut Vec<String>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(mut source_errors) => {
            errors.append(&mut source_errors);
            None
        }
    }
}

fn validate_coverage(
    mode: Mode,
    matrix: &MatrixDocument,
    spec: &SpecDocument,
    coverage: &CoverageDocument,
    test_list: &TestList,
    errors: &mut Vec<String>,
) -> Report {
    if coverage.schema_version != 1 {
        errors.push(format!(
            "unsupported coverage schema_version {}; expected 1",
            coverage.schema_version
        ));
    }

    let mut by_id = BTreeMap::new();
    for case in &coverage.cases {
        if by_id.insert(case.id.as_str(), case).is_some() {
            errors.push(format!("coverage contains duplicate case ID `{}`", case.id));
        }
    }

    for id in &matrix.cases {
        if !by_id.contains_key(id.as_str()) {
            errors.push(format!("matrix case `{id}` is missing from coverage"));
        }
    }
    for id in by_id.keys() {
        if !matrix.cases.contains(*id) {
            errors.push(format!("coverage contains unknown case ID `{id}`"));
        }
    }

    let mut executable = 0;
    let mut partial = Vec::new();
    let mut gap = Vec::new();
    let mut out_of_scope = Vec::new();

    for case in by_id.values() {
        validate_case(case, test_list, errors);
        let detail = StatusDetail {
            id: case.id.clone(),
            reason: case.reason.trim().to_owned(),
        };
        match case.status {
            CoverageStatus::Executable => executable += 1,
            CoverageStatus::Partial => partial.push(detail),
            CoverageStatus::Gap => gap.push(detail),
            CoverageStatus::OutOfScope => out_of_scope.push(detail),
        }
    }

    if mode == Mode::Gate {
        if !partial.is_empty() {
            errors.push(format!(
                "gate rejects {} partial case(s): {}",
                partial.len(),
                join_ids(&partial)
            ));
        }
        if !gap.is_empty() {
            errors.push(format!(
                "gate rejects {} gap case(s): {}",
                gap.len(),
                join_ids(&gap)
            ));
        }
    }

    Report {
        matrix_cases: matrix.cases.len(),
        declared_spec_ids: spec.requirements.len(),
        linked_requirements: matrix.requirements.len(),
        listed_rust_tests: test_list.len(),
        executable,
        partial,
        gap,
        out_of_scope,
    }
}

fn validate_case(case: &CoverageCase, test_list: &TestList, errors: &mut Vec<String>) {
    let reason_present = !case.reason.trim().is_empty();
    match case.status {
        CoverageStatus::Executable => {
            if case.rust.is_empty() {
                errors.push(format!(
                    "coverage case `{}` is executable but has no Rust evidence",
                    case.id
                ));
            }
        }
        CoverageStatus::Partial => {
            if case.rust.is_empty() {
                errors.push(format!(
                    "coverage case `{}` is partial but has no Rust evidence",
                    case.id
                ));
            }
            if !reason_present {
                errors.push(format!(
                    "coverage case `{}` is partial but has no reason",
                    case.id
                ));
            }
        }
        CoverageStatus::Gap | CoverageStatus::OutOfScope => {
            if !case.rust.is_empty() {
                errors.push(format!(
                    "coverage case `{}` has status {} but declares Rust evidence",
                    case.id, case.status
                ));
            }
            if !reason_present {
                errors.push(format!(
                    "coverage case `{}` has status {} but has no reason",
                    case.id, case.status
                ));
            }
        }
    }

    validate_unique_nonempty_values(&case.id, "rust", &case.rust, errors);

    for evidence in &case.rust {
        match test_list.count(evidence) {
            0 => errors.push(format!(
                "coverage case `{}` references missing Rust test `{evidence}`",
                case.id
            )),
            1 => {}
            count => errors.push(format!(
                "coverage case `{}` references ambiguous Rust test `{evidence}` listed {count} times",
                case.id
            )),
        }
    }
}

fn validate_unique_nonempty_values(
    case_id: &str,
    field: &str,
    values: &[String],
    errors: &mut Vec<String>,
) {
    let mut seen = BTreeSet::new();
    for value in values {
        if value.trim().is_empty() {
            errors.push(format!(
                "coverage case `{case_id}` contains an empty `{field}` value"
            ));
        } else if !seen.insert(value) {
            errors.push(format!(
                "coverage case `{case_id}` contains duplicate `{field}` value `{value}`"
            ));
        }
    }
}

fn join_ids(details: &[StatusDetail]) -> String {
    details
        .iter()
        .map(|detail| detail.id.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}
