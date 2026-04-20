#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegexLite {
    anchored_start: bool,
    anchored_end: bool,
    prefix: String,
    branches: Vec<String>,
    suffix: String,
}

impl RegexLite {
    pub(crate) fn compile(pattern: &str) -> Result<Self, RegexLiteError> {
        let mut anchored_start = false;
        let mut start = 0;
        if pattern.starts_with('^') {
            anchored_start = true;
            start = 1;
        }

        let mut anchored_end = false;
        let mut end = pattern.len();
        if let Some(index) = trailing_anchor_index(pattern) {
            if index >= start {
                anchored_end = true;
                end = index;
            }
        }

        let body = &pattern[start..end];
        let mut prefix = String::new();
        let mut suffix = String::new();
        let mut branches = Vec::new();
        let mut branch = String::new();
        let mut group_open = false;
        let mut group_seen = false;
        let mut escaped = false;

        for ch in body.chars() {
            if escaped {
                current_segment(
                    group_seen,
                    group_open,
                    &mut prefix,
                    &mut suffix,
                    &mut branch,
                )
                .push(ch);
                escaped = false;
                continue;
            }

            match ch {
                '\\' => escaped = true,
                '(' if !group_open && !group_seen => group_open = true,
                '(' => return Err(RegexLiteError::new("nested groups are not supported")),
                ')' if group_open => {
                    if branch.is_empty() {
                        return Err(RegexLiteError::new(
                            "empty alternation branches are not supported",
                        ));
                    }
                    branches.push(std::mem::take(&mut branch));
                    group_open = false;
                    group_seen = true;
                }
                ')' => return Err(RegexLiteError::new("unexpected `)`")),
                '|' if group_open => {
                    if branch.is_empty() {
                        return Err(RegexLiteError::new(
                            "empty alternation branches are not supported",
                        ));
                    }
                    branches.push(std::mem::take(&mut branch));
                }
                '|' => {
                    return Err(RegexLiteError::new(
                        "`|` is only supported inside one group",
                    ));
                }
                '^' | '$' => {
                    return Err(RegexLiteError::new(
                        "anchors are only supported at the beginning or end",
                    ));
                }
                '.' | '*' | '+' | '?' | '[' | ']' | '{' | '}' => {
                    return Err(RegexLiteError::new(format!(
                        "unsupported regex token `{ch}`"
                    )));
                }
                _ => current_segment(
                    group_seen,
                    group_open,
                    &mut prefix,
                    &mut suffix,
                    &mut branch,
                )
                .push(ch),
            }
        }

        if escaped {
            return Err(RegexLiteError::new("trailing escape is not supported"));
        }
        if group_open {
            return Err(RegexLiteError::new("unclosed alternation group"));
        }

        if !group_seen {
            branches.push(prefix.clone());
            prefix.clear();
        }
        if branches.is_empty() {
            branches.push(String::new());
        }

        Ok(Self {
            anchored_start,
            anchored_end,
            prefix,
            branches,
            suffix,
        })
    }

    pub(crate) fn is_match(&self, input: &str) -> bool {
        self.branches.iter().any(|branch| {
            match_variant(
                input,
                &self.prefix,
                branch,
                &self.suffix,
                self.anchored_start,
                self.anchored_end,
            )
        })
    }
}

fn current_segment<'a>(
    group_seen: bool,
    group_open: bool,
    prefix: &'a mut String,
    suffix: &'a mut String,
    branch: &'a mut String,
) -> &'a mut String {
    if group_open {
        branch
    } else if group_seen {
        suffix
    } else {
        prefix
    }
}

fn trailing_anchor_index(pattern: &str) -> Option<usize> {
    let (index, ch) = pattern.char_indices().last()?;
    if ch != '$' {
        return None;
    }
    let backslashes = pattern[..index]
        .chars()
        .rev()
        .take_while(|ch| *ch == '\\')
        .count();
    (backslashes % 2 == 0).then_some(index)
}

fn match_variant(
    input: &str,
    prefix: &str,
    branch: &str,
    suffix: &str,
    anchored_start: bool,
    anchored_end: bool,
) -> bool {
    let candidate = format!("{prefix}{branch}{suffix}");
    match (anchored_start, anchored_end) {
        (true, true) => input == candidate,
        (true, false) => input.starts_with(&candidate),
        (false, true) => input.ends_with(&candidate),
        (false, false) => input.contains(&candidate),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegexLiteError {
    message: String,
}

impl RegexLiteError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RegexLiteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RegexLiteError {}

#[cfg(test)]
mod tests {
    use super::RegexLite;

    #[test]
    fn matches_literals_and_anchors() {
        let exact = RegexLite::compile("^Ada$").unwrap();
        assert!(exact.is_match("Ada"));
        assert!(!exact.is_match("Ada Lovelace"));

        let suffix = RegexLite::compile("lin$").unwrap();
        assert!(suffix.is_match("Merlin"));
    }

    #[test]
    fn matches_single_alternation_group() {
        let regex = RegexLite::compile("^(KNOWS|MENTORS)$").unwrap();
        assert!(regex.is_match("KNOWS"));
        assert!(regex.is_match("MENTORS"));
        assert!(!regex.is_match("LIKES"));
    }

    #[test]
    fn supports_escaped_metacharacters() {
        let regex = RegexLite::compile(r"^\(Ada\)$").unwrap();
        assert!(regex.is_match("(Ada)"));
        assert!(!regex.is_match("Ada"));
    }

    #[test]
    fn rejects_unsupported_patterns() {
        assert!(RegexLite::compile("(").is_err());
        assert!(RegexLite::compile("a.*b").is_err());
        assert!(RegexLite::compile("(Ada|)").is_err());
    }
}
