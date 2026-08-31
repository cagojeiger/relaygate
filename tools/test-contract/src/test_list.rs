use std::collections::BTreeMap;

#[derive(Debug)]
pub(crate) struct TestList {
    pub(crate) counts: BTreeMap<String, usize>,
}

impl TestList {
    pub(crate) fn count(&self, name: &str) -> usize {
        self.counts.get(name).copied().unwrap_or_default()
    }

    pub(crate) fn len(&self) -> usize {
        self.counts.len()
    }
}

pub(crate) fn parse(source: &str) -> Result<TestList, Vec<String>> {
    let mut counts = BTreeMap::new();
    for line in source.lines() {
        let trimmed = line.trim();
        let Some(name) = trimmed.strip_suffix(": test") else {
            continue;
        };
        if !name.is_empty() {
            *counts.entry(name.to_owned()).or_insert(0) += 1;
        }
    }

    if counts.is_empty() {
        Err(vec![
            "cargo test list contains no lines ending in `: test`".to_owned(),
        ])
    } else {
        Ok(TestList { counts })
    }
}
