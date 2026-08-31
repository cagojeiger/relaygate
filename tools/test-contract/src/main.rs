use relaygate_test_contract::{Mode, Report, SpecSource, StatusDetail, validate};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const USAGE: &str = "Usage: relaygate-test-contract <check|gate> --matrix <TEST-001.md> --coverage <coverage.toml> --test-list <cargo-test-list.txt> --spec-dir <docs/spec>";

struct Arguments {
    mode: Mode,
    matrix: PathBuf,
    coverage: PathBuf,
    test_list: PathBuf,
    spec_dir: PathBuf,
}

fn main() -> ExitCode {
    let arguments = match parse_arguments(env::args().skip(1)) {
        Ok(arguments) => arguments,
        Err(error) => {
            eprintln!("error: {error}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    let matrix = match read_file("matrix", &arguments.matrix) {
        Ok(source) => source,
        Err(error) => return fail_io(error),
    };
    let coverage = match read_file("coverage", &arguments.coverage) {
        Ok(source) => source,
        Err(error) => return fail_io(error),
    };
    let test_list = match read_file("test list", &arguments.test_list) {
        Ok(source) => source,
        Err(error) => return fail_io(error),
    };
    let specs = match read_specs(&arguments.spec_dir) {
        Ok(specs) => specs,
        Err(error) => return fail_io(error),
    };
    let spec_sources: Vec<_> = specs
        .iter()
        .map(|(name, contents)| SpecSource { name, contents })
        .collect();

    match validate(
        arguments.mode,
        &matrix,
        &coverage,
        &test_list,
        &spec_sources,
    ) {
        Ok(report) => {
            print_report(arguments.mode, &report);
            ExitCode::SUCCESS
        }
        Err(errors) => {
            for error in &errors {
                eprintln!("error: {error}");
            }
            eprintln!("contract validation failed with {} error(s)", errors.len());
            ExitCode::FAILURE
        }
    }
}

fn parse_arguments(arguments: impl Iterator<Item = String>) -> Result<Arguments, String> {
    let mut arguments = arguments.peekable();
    let mode = match arguments.next().as_deref() {
        Some("check") => Mode::Check,
        Some("gate") => Mode::Gate,
        Some("--help" | "-h") => return Err("help requested".to_owned()),
        Some(value) => {
            return Err(format!(
                "unknown mode `{value}`; expected `check` or `gate`"
            ));
        }
        None => return Err("missing mode; expected `check` or `gate`".to_owned()),
    };

    let mut matrix = None;
    let mut coverage = None;
    let mut test_list = None;
    let mut spec_dir = None;
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for `{flag}`"))?;
        match flag.as_str() {
            "--matrix" => set_once(&mut matrix, value, "--matrix")?,
            "--coverage" => set_once(&mut coverage, value, "--coverage")?,
            "--test-list" => set_once(&mut test_list, value, "--test-list")?,
            "--spec-dir" => set_once(&mut spec_dir, value, "--spec-dir")?,
            _ => return Err(format!("unknown argument `{flag}`")),
        }
    }

    Ok(Arguments {
        mode,
        matrix: required_path(matrix, "--matrix")?,
        coverage: required_path(coverage, "--coverage")?,
        test_list: required_path(test_list, "--test-list")?,
        spec_dir: required_path(spec_dir, "--spec-dir")?,
    })
}

fn set_once(target: &mut Option<String>, value: String, flag: &str) -> Result<(), String> {
    if target.replace(value).is_some() {
        Err(format!("duplicate argument `{flag}`"))
    } else {
        Ok(())
    }
}

fn required_path(value: Option<String>, flag: &str) -> Result<PathBuf, String> {
    value
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing required `{flag}`"))
}

fn read_file(label: &str, path: &Path) -> Result<String, String> {
    fs::read_to_string(path)
        .map_err(|error| format!("failed to read {label} `{}`: {error}", path.display()))
}

fn read_specs(spec_dir: &Path) -> Result<Vec<(String, String)>, String> {
    let entries = fs::read_dir(spec_dir).map_err(|error| {
        format!(
            "failed to read SPEC directory `{}`: {error}",
            spec_dir.display()
        )
    })?;
    let mut by_number = BTreeMap::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "failed to read an entry in SPEC directory `{}`: {error}",
                spec_dir.display()
            )
        })?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("md") {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(prefix) = name.get(..3) else {
            continue;
        };
        let Ok(number) = prefix.parse::<u8>() else {
            continue;
        };
        if !(1..=8).contains(&number) || name.as_bytes().get(3) != Some(&b'-') {
            continue;
        }
        if by_number.insert(number, path).is_some() {
            return Err(format!(
                "SPEC directory `{}` contains more than one document with prefix {number:03}",
                spec_dir.display()
            ));
        }
    }

    let mut specs = Vec::new();
    for number in 1_u8..=8 {
        let path = by_number.remove(&number).ok_or_else(|| {
            format!(
                "SPEC directory `{}` is missing document {number:03}-*.md",
                spec_dir.display()
            )
        })?;
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("SPEC path `{}` is not valid UTF-8", path.display()))?
            .to_owned();
        let contents = read_file("SPEC document", &path)?;
        specs.push((name, contents));
    }
    Ok(specs)
}

fn fail_io(error: String) -> ExitCode {
    eprintln!("error: {error}");
    ExitCode::FAILURE
}

fn print_report(mode: Mode, report: &Report) {
    let mode = match mode {
        Mode::Check => "check",
        Mode::Gate => "gate",
    };
    println!("contract {mode} passed");
    println!("matrix cases: {}", report.matrix_cases);
    println!("declared SPEC IDs: {}", report.declared_spec_ids);
    println!("linked SPEC IDs: {}", report.linked_requirements);
    println!("listed Rust tests: {}", report.listed_rust_tests);
    println!("executable: {}", report.executable);
    print_details("partial", &report.partial);
    print_details("gap", &report.gap);
    print_details("out_of_scope", &report.out_of_scope);
}

fn print_details(label: &str, details: &[StatusDetail]) {
    println!("{label}: {}", details.len());
    for detail in details {
        println!("  - {}: {}", detail.id, detail.reason);
    }
}
