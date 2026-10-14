// Copyright 2026 The Roc Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! String row functions of the kernel model: substrings, case, concatenation
//! and LIKE patterns, over UTF-8 byte strings.

/// The characters at 1-based positions `start` to `start + count - 1` that
/// exist, as a byte range of `text`; None for a negative count.
pub(crate) fn substring(text: &[u8], start: i64, count: i64) -> Option<(usize, usize)> {
    if count < 0 {
        return None;
    }
    let first = start.saturating_sub(1);
    let end = first.saturating_add(count);
    let (first, end) = (first.max(0) as u64, end.max(0) as u64);
    if end <= first {
        return Some((0, 0));
    }
    if text.is_ascii() {
        let len = text.len() as u64;
        return Some((first.min(len) as usize, end.min(len) as usize));
    }
    let (mut from, mut to) = (text.len(), text.len());
    let mut chars = 0u64;
    for (i, &b) in text.iter().enumerate() {
        // Each character starts with a byte that is not a continuation byte.
        if b & 0xC0 != 0x80 {
            if chars == first {
                from = i;
            }
            if chars == end {
                to = i;
                break;
            }
            chars += 1;
        }
    }
    Some((from, to.max(from)))
}

/// A compiled LIKE pattern: segments between `%`, each a sequence of
/// literal characters and `_`.
#[derive(Debug)]
pub(crate) struct Pattern {
    segments: Vec<Vec<Token>>,
    /// Whether the pattern starts and ends with `%`.
    open_start: bool,
    open_end: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token {
    Char(char),
    Any,
}

impl Pattern {
    pub fn new(pattern: &str, escape: Option<char>) -> Self {
        let mut segments = vec![vec![]];
        let mut chars = pattern.chars();
        let mut open_start = false;
        let mut open_end = false;
        let mut first = true;
        while let Some(c) = chars.next() {
            if Some(c) == escape {
                let literal = chars.next().unwrap_or(c);
                segments.last_mut().unwrap().push(Token::Char(literal));
            } else if c == '%' {
                if first {
                    open_start = true;
                }
                if !segments.last().unwrap().is_empty() {
                    segments.push(vec![]);
                }
                open_end = true;
                first = false;
                continue;
            } else if c == '_' {
                segments.last_mut().unwrap().push(Token::Any);
            } else {
                segments.last_mut().unwrap().push(Token::Char(c));
            }
            open_end = false;
            first = false;
        }
        if segments.last().is_some_and(|s| s.is_empty()) && segments.len() > 1 {
            segments.pop();
        }
        Self {
            segments,
            open_start,
            open_end,
        }
    }

    pub fn matches(&self, text: &str) -> bool {
        let chars = text.chars().collect::<Vec<_>>();
        let at = |s: &[Token], i: usize| {
            i + s.len() <= chars.len()
                && s.iter().zip(&chars[i..]).all(|(t, c)| match t {
                    Token::Any => true,
                    Token::Char(p) => p == c,
                })
        };
        let n = self.segments.len();
        if n == 1 && !self.open_start && !self.open_end {
            return self.segments[0].len() == chars.len() && at(&self.segments[0], 0);
        }
        let mut pos = 0;
        let mut first = 0;
        let mut last = n;
        if !self.open_start {
            if !at(&self.segments[0], 0) {
                return false;
            }
            pos = self.segments[0].len();
            first = 1;
        }
        let tail = if !self.open_end && last > first {
            last -= 1;
            Some(&self.segments[last])
        } else {
            None
        };
        for segment in &self.segments[first..last] {
            match (pos..=chars.len().saturating_sub(segment.len())).find(|&i| at(segment, i)) {
                Some(i) if i + segment.len() <= chars.len() => pos = i + segment.len(),
                _ => return false,
            }
        }
        match tail {
            Some(t) => chars.len() >= pos + t.len() && at(t, chars.len() - t.len()),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_patterns_match_like_sql() {
        let cases = [
            ("0-500%", "0-500", true),
            ("0-500%", "0-5000x", true),
            ("0-500%", "1-500", false),
            ("%ab%", "xxabyy", true),
            ("%ab%", "xaxb", false),
            ("a_c", "abc", true),
            ("a_c", "abbc", false),
            ("a%c", "ac", true),
            ("a%c", "abxc", true),
            ("a%c", "abxcd", false),
            ("%", "", true),
            ("", "", true),
            ("", "a", false),
            ("_é%", "xé1", true),
            ("%c", "abc", true),
            ("%c", "abd", false),
            ("a%b%c", "aXbYc", true),
            ("a%b%c", "aXcYb", false),
            ("ab%%", "ab", true),
        ];
        for (p, t, expected) in cases {
            assert_eq!(Pattern::new(p, None).matches(t), expected, "{t} LIKE {p}");
        }
        assert!(Pattern::new("a\\%b", Some('\\')).matches("a%b"));
        assert!(!Pattern::new("a\\%b", Some('\\')).matches("axb"));
    }

    #[test]
    fn substrings_count_characters_from_one() {
        fn s(t: &str, start: i64, count: i64) -> Option<&str> {
            substring(t.as_bytes(), start, count).map(|(a, b)| &t[a..b])
        }
        assert_eq!(s("hello", 1, 5), Some("hello"));
        assert_eq!(s("hello", 0, 2), Some("h"));
        assert_eq!(s("hello", -3, 5), Some("h"));
        assert_eq!(s("hello", 4, 10), Some("lo"));
        assert_eq!(s("hello", 9, 1), Some(""));
        assert_eq!(s("héllo", 2, 2), Some("él"));
        assert_eq!(s("hello", 2, -1), None);
        assert_eq!(s("hello", i64::MIN, 3), Some(""));
    }
}
