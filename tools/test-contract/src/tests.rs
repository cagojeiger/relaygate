use crate::{Mode, Report, SpecSource, validate};

const MATRIX: &str = r#"
| Test ID | Requirement | scenario |
| --- | --- | --- |
| `T-SDK-01` | `SDK-001`, `STATE-SDK-001` | executable |

| Test ID | fault | expected |
| --- | --- | --- |
| `T-EDGE-01` | disconnect | cleanup |
"#;

const TEST_LIST: &str = r#"
tests::connect: test
tests::disconnect: test
"#;

const SPECS: &[SpecSource<'_>] = &[
    SpecSource {
        name: "001.md",
        contents: "- **`SDK-001`**: sdk rule",
    },
    SpecSource {
        name: "007.md",
        contents: "| `STATE-SDK-001` | state rule |",
    },
];

#[test]
fn accepts_complete_exact_evidence_in_gate_mode() -> Result<(), String> {
    let coverage = coverage(
        r#"
[[case]]
id = "T-SDK-01"
status = "executable"
rust = ["tests::connect"]

[[case]]
id = "T-EDGE-01"
status = "executable"
rust = ["tests::disconnect"]
"#,
    );

    let report = valid(Mode::Gate, MATRIX, &coverage, TEST_LIST, SPECS)?;
    assert_eq!(report.matrix_cases, 2);
    assert_eq!(report.declared_spec_ids, 2);
    assert_eq!(report.linked_requirements, 2);
    assert_eq!(report.executable, 2);
    Ok(())
}

#[test]
fn check_reports_partial_and_gap_but_gate_rejects_them() -> Result<(), String> {
    let coverage = coverage(
        r#"
[[case]]
id = "T-SDK-01"
status = "partial"
rust = ["tests::connect"]
reason = "timeout order is not covered"

[[case]]
id = "T-EDGE-01"
status = "gap"
reason = "scripted disconnect is missing"
"#,
    );

    let report = valid(Mode::Check, MATRIX, &coverage, TEST_LIST, SPECS)?;
    assert_eq!(report.partial.len(), 1);
    assert_eq!(report.gap.len(), 1);

    let errors = invalid(Mode::Gate, MATRIX, &coverage, TEST_LIST, SPECS)?;
    assert!(
        errors
            .iter()
            .any(|error| error.contains("gate rejects 1 partial"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("gate rejects 1 gap"))
    );
    Ok(())
}

#[test]
fn rejects_missing_duplicate_and_unknown_coverage_cases() -> Result<(), String> {
    let coverage = coverage(
        r#"
[[case]]
id = "T-SDK-01"
status = "executable"
rust = ["tests::connect"]

[[case]]
id = "T-SDK-01"
status = "executable"
rust = ["tests::connect"]

[[case]]
id = "T-UNKNOWN-01"
status = "gap"
reason = "not in matrix"
"#,
    );

    let errors = invalid(Mode::Check, MATRIX, &coverage, TEST_LIST, SPECS)?;
    assert!(
        errors
            .iter()
            .any(|error| error.contains("duplicate case ID `T-SDK-01`"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("`T-EDGE-01` is missing"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("unknown case ID `T-UNKNOWN-01`"))
    );
    Ok(())
}

#[test]
fn rejects_invalid_status_invariants() -> Result<(), String> {
    let coverage = coverage(
        r#"
[[case]]
id = "T-SDK-01"
status = "partial"

[[case]]
id = "T-EDGE-01"
status = "out_of_scope"
rust = ["tests::disconnect"]
"#,
    );

    let errors = invalid(Mode::Check, MATRIX, &coverage, TEST_LIST, SPECS)?;
    assert!(
        errors
            .iter()
            .any(|error| error.contains("partial but has no Rust evidence"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("partial but has no reason"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("declares Rust evidence"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("out_of_scope but has no reason"))
    );
    Ok(())
}

#[test]
fn rejects_missing_and_ambiguous_test_evidence() -> Result<(), String> {
    let coverage = coverage(
        r#"
[[case]]
id = "T-SDK-01"
status = "executable"
rust = ["tests::connect"]

[[case]]
id = "T-EDGE-01"
status = "executable"
rust = ["tests::missing"]
"#,
    );
    let test_list = r#"
tests::connect: test
tests::connect: test
tests::disconnect: test
"#;

    let errors = invalid(Mode::Check, MATRIX, &coverage, test_list, SPECS)?;
    assert!(
        errors
            .iter()
            .any(|error| error.contains("references ambiguous Rust test"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("references missing Rust test"))
    );
    Ok(())
}

#[test]
fn ignores_duplicate_unreferenced_test_names() -> Result<(), String> {
    let coverage = coverage(
        r#"
[[case]]
id = "T-SDK-01"
status = "executable"
rust = ["tests::connect"]

[[case]]
id = "T-EDGE-01"
status = "executable"
rust = ["tests::disconnect"]
"#,
    );
    let test_list = r#"
tests::connect: test
tests::disconnect: test
tests::unreferenced: test
tests::unreferenced: test
"#;

    valid(Mode::Check, MATRIX, &coverage, test_list, SPECS)?;
    Ok(())
}

#[test]
fn rejects_requirement_rows_without_linked_spec_ids() -> Result<(), String> {
    let matrix = r#"
| Test ID | Requirement | scenario |
| --- | --- | --- |
| `T-SDK-01` | none | missing link |
"#;
    let coverage = coverage(
        r#"
[[case]]
id = "T-SDK-01"
status = "executable"
rust = ["tests::connect"]
"#,
    );

    let errors = invalid(Mode::Check, matrix, &coverage, TEST_LIST, SPECS)?;
    assert!(errors.iter().any(|error| error.contains("no linked SPEC")));
    Ok(())
}

#[test]
fn rejects_spec_and_matrix_requirement_set_mismatch() -> Result<(), String> {
    let coverage = coverage(
        r#"
[[case]]
id = "T-SDK-01"
status = "executable"
rust = ["tests::connect"]

[[case]]
id = "T-EDGE-01"
status = "executable"
rust = ["tests::disconnect"]
"#,
    );
    let specs = [
        SpecSource {
            name: "001.md",
            contents: "- **`SDK-001`**: sdk rule\n- **`SDK-999`**: missing rule",
        },
        SpecSource {
            name: "007.md",
            contents: "| `STATE-OTHER-001` | unknown state |",
        },
    ];

    let errors = invalid(Mode::Check, MATRIX, &coverage, TEST_LIST, &specs)?;
    assert!(
        errors
            .iter()
            .any(|error| error.contains("`SDK-999` is not linked"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("`STATE-OTHER-001` is not linked"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("unknown SPEC requirement/state ID `STATE-SDK-001`"))
    );
    Ok(())
}

#[test]
fn rejects_unknown_toml_fields_and_schema_versions() -> Result<(), String> {
    let unknown_field = r#"
schema_version = 1
unexpected = true
case = []
"#;
    let errors = invalid(Mode::Check, MATRIX, unknown_field, TEST_LIST, SPECS)?;
    assert!(
        errors
            .iter()
            .any(|error| error.contains("coverage TOML parse error"))
    );

    let unsupported = coverage(
        r#"
[[case]]
id = "T-SDK-01"
status = "executable"
rust = ["tests::connect"]

[[case]]
id = "T-EDGE-01"
status = "executable"
rust = ["tests::disconnect"]
"#,
    )
    .replacen("schema_version = 1", "schema_version = 2", 1);
    let errors = invalid(Mode::Check, MATRIX, &unsupported, TEST_LIST, SPECS)?;
    assert!(
        errors
            .iter()
            .any(|error| error.contains("schema_version 2"))
    );
    Ok(())
}

fn valid(
    mode: Mode,
    matrix: &str,
    coverage: &str,
    test_list: &str,
    specs: &[SpecSource<'_>],
) -> Result<Report, String> {
    validate(mode, matrix, coverage, test_list, specs).map_err(|errors| errors.join("; "))
}

fn invalid(
    mode: Mode,
    matrix: &str,
    coverage: &str,
    test_list: &str,
    specs: &[SpecSource<'_>],
) -> Result<Vec<String>, String> {
    match validate(mode, matrix, coverage, test_list, specs) {
        Ok(_) => Err("validation unexpectedly succeeded".to_owned()),
        Err(errors) => Ok(errors),
    }
}

fn coverage(cases: &str) -> String {
    format!("schema_version = 1\n{cases}")
}
