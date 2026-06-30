// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::spec::{DataField, DataType, Datum, Predicate, PredicateOperator};
use std::cmp::Ordering;

pub(crate) trait StatsAccessor {
    fn row_count(&self) -> i64;
    fn null_count(&self, index: usize) -> Option<i64>;
    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum>;
    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum>;
}

pub(crate) fn predicates_may_match_with_schema<T: StatsAccessor>(
    predicates: &[Predicate],
    stats: &T,
    field_mapping: &[Option<usize>],
    file_fields: &[DataField],
) -> bool {
    predicates.iter().all(|predicate| {
        predicate_may_match_with_schema(predicate, stats, field_mapping, file_fields)
    })
}

pub(crate) fn data_leaf_may_match<T: StatsAccessor>(
    index: usize,
    stats_data_type: &DataType,
    predicate_data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
    stats: &T,
) -> bool {
    let row_count = stats.row_count();
    if row_count <= 0 {
        return false;
    }

    let null_count = stats.null_count(index);
    let all_null = null_count.map(|count| count == row_count);

    match op {
        PredicateOperator::IsNull => {
            return null_count.is_none_or(|count| count > 0);
        }
        PredicateOperator::IsNotNull => {
            return all_null != Some(true);
        }
        PredicateOperator::In | PredicateOperator::NotIn => {
            return true;
        }
        PredicateOperator::EndsWith | PredicateOperator::Contains => {
            // String min/max ordering carries no information about suffix /
            // substring matches, so fail open.
            return true;
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq
        | PredicateOperator::StartsWith
        | PredicateOperator::Like => {}
    }

    if all_null == Some(true) {
        return false;
    }

    let literal = match literals.first() {
        Some(literal) => literal,
        None => return true,
    };

    let min_value = match stats
        .min_value(index, stats_data_type)
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };
    let max_value = match stats
        .max_value(index, stats_data_type)
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };

    match op {
        PredicateOperator::Eq => {
            !matches!(literal.partial_cmp(&min_value), Some(Ordering::Less))
                && !matches!(literal.partial_cmp(&max_value), Some(Ordering::Greater))
        }
        PredicateOperator::NotEq => !(min_value == *literal && max_value == *literal),
        PredicateOperator::Lt => !matches!(
            min_value.partial_cmp(literal),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        PredicateOperator::LtEq => {
            !matches!(min_value.partial_cmp(literal), Some(Ordering::Greater))
        }
        PredicateOperator::Gt => !matches!(
            max_value.partial_cmp(literal),
            Some(Ordering::Less | Ordering::Equal)
        ),
        PredicateOperator::GtEq => !matches!(max_value.partial_cmp(literal), Some(Ordering::Less)),
        PredicateOperator::StartsWith => {
            // pat lives in [min, max] iff max >= pat AND min < pat_next, where
            // pat_next is pat with its last codepoint incremented. If we can't
            // compute pat_next (last char is char::MAX, increments into the
            // UTF-16 surrogate range, etc.), fail open.
            let (pat, min_str, max_str) = match (literal, &min_value, &max_value) {
                (Datum::String(p), Datum::String(lo), Datum::String(hi)) => {
                    (p.as_str(), lo.as_str(), hi.as_str())
                }
                _ => return true,
            };
            // If the file's max is below the pattern (lexicographically), no
            // string in the file can start with `pat`.
            if max_str < pat {
                return false;
            }
            // Compute pat_next; if we can, use the [pat, pat_next) range to
            // also rule out files whose min is already past every pat-prefixed
            // string. Otherwise just trust the upper bound check.
            match next_string_for_prefix(pat) {
                Some(pat_next) => min_str.as_bytes() < pat_next.as_slice(),
                None => true,
            }
        }
        PredicateOperator::Like => {
            // Try to extract a literal prefix from the LIKE pattern (the
            // characters before the first unescaped wildcard). If we get one,
            // prune as if it were StartsWith; otherwise fail open.
            let (pattern, min_str, max_str) = match (literal, &min_value, &max_value) {
                (Datum::String(p), Datum::String(lo), Datum::String(hi)) => {
                    (p.as_str(), lo.as_str(), hi.as_str())
                }
                _ => return true,
            };
            let Some(pat) = like_pattern_literal_prefix(pattern) else {
                return true;
            };
            if pat.is_empty() {
                return true;
            }
            if max_str < pat.as_str() {
                return false;
            }
            match next_string_for_prefix(&pat) {
                Some(pat_next) => min_str.as_bytes() < pat_next.as_slice(),
                None => true,
            }
        }
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::In
        | PredicateOperator::NotIn
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains => true,
    }
}

/// Return the literal prefix of a SQL LIKE pattern up to the first unescaped
/// `%` or `_`. A backslash escapes the next character (which is appended
/// literally, mirroring arrow's `like` kernel); a trailing backslash is a
/// literal backslash.
fn like_pattern_literal_prefix(pattern: &str) -> Option<String> {
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' | '_' => return Some(out),
            '\\' => match chars.next() {
                Some(next) => out.push(next),
                None => out.push('\\'),
            },
            other => out.push(other),
        }
    }
    Some(out)
}

/// Compute the smallest string strictly greater than every string with `prefix`
/// as a prefix, by incrementing the last codepoint. Returns `None` if the last
/// codepoint cannot be incremented within valid Unicode (e.g. `char::MAX`).
fn next_string_for_prefix(prefix: &str) -> Option<Vec<u8>> {
    let last_char = prefix.chars().next_back()?;
    let mut next_code = last_char as u32 + 1;
    // Skip over the UTF-16 surrogate range, which is not valid scalar Unicode.
    if (0xD800..=0xDFFF).contains(&next_code) {
        next_code = 0xE000;
    }
    let next_char = char::from_u32(next_code)?;
    let mut bytes = prefix.as_bytes()[..prefix.len() - last_char.len_utf8()].to_vec();
    let mut buf = [0u8; 4];
    bytes.extend_from_slice(next_char.encode_utf8(&mut buf).as_bytes());
    Some(bytes)
}

pub(crate) fn missing_field_may_match(op: PredicateOperator, row_count: i64) -> bool {
    if row_count <= 0 {
        return false;
    }

    matches!(op, PredicateOperator::IsNull)
}

fn predicate_may_match_with_schema<T: StatsAccessor>(
    predicate: &Predicate,
    stats: &T,
    field_mapping: &[Option<usize>],
    file_fields: &[DataField],
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children
            .iter()
            .all(|child| predicate_may_match_with_schema(child, stats, field_mapping, file_fields)),
        Predicate::Or(_) | Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => match field_mapping.get(*index).copied().flatten() {
            Some(file_index) => {
                let Some(file_field) = file_fields.get(file_index) else {
                    return true;
                };
                data_leaf_may_match(
                    file_index,
                    file_field.data_type(),
                    data_type,
                    *op,
                    literals,
                    stats,
                )
            }
            None => missing_field_may_match(*op, stats.row_count()),
        },
    }
}

fn coerce_stats_datum_for_predicate(datum: Datum, predicate_data_type: &DataType) -> Option<Datum> {
    match (datum, predicate_data_type) {
        (datum @ Datum::Bool(_), DataType::Boolean(_))
        | (datum @ Datum::TinyInt(_), DataType::TinyInt(_))
        | (datum @ Datum::SmallInt(_), DataType::SmallInt(_))
        | (datum @ Datum::Int(_), DataType::Int(_))
        | (datum @ Datum::Long(_), DataType::BigInt(_))
        | (datum @ Datum::Float(_), DataType::Float(_))
        | (datum @ Datum::Double(_), DataType::Double(_))
        | (datum @ Datum::String(_), DataType::VarChar(_))
        | (datum @ Datum::String(_), DataType::Char(_))
        | (datum @ Datum::Bytes(_), DataType::Binary(_))
        | (datum @ Datum::Bytes(_), DataType::VarBinary(_))
        | (datum @ Datum::Date(_), DataType::Date(_))
        | (datum @ Datum::Time(_), DataType::Time(_))
        | (datum @ Datum::Timestamp { .. }, DataType::Timestamp(_))
        | (datum @ Datum::LocalZonedTimestamp { .. }, DataType::LocalZonedTimestamp(_))
        | (datum @ Datum::Decimal { .. }, DataType::Decimal(_)) => Some(datum),
        (Datum::TinyInt(value), DataType::SmallInt(_)) => Some(Datum::SmallInt(value as i16)),
        (Datum::TinyInt(value), DataType::Int(_)) => Some(Datum::Int(value as i32)),
        (Datum::TinyInt(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::SmallInt(value), DataType::Int(_)) => Some(Datum::Int(value as i32)),
        (Datum::SmallInt(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::Int(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::Float(value), DataType::Double(_)) => Some(Datum::Double(value as f64)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, VarCharType};

    struct MockStats {
        row_count: i64,
        null_count: Option<i64>,
        min: Option<Datum>,
        max: Option<Datum>,
    }

    impl StatsAccessor for MockStats {
        fn row_count(&self) -> i64 {
            self.row_count
        }
        fn null_count(&self, _index: usize) -> Option<i64> {
            self.null_count
        }
        fn min_value(&self, _index: usize, _data_type: &DataType) -> Option<Datum> {
            self.min.clone()
        }
        fn max_value(&self, _index: usize, _data_type: &DataType) -> Option<Datum> {
            self.max.clone()
        }
    }

    fn varchar() -> DataType {
        DataType::VarChar(VarCharType::default())
    }

    fn string_stats(min: &str, max: &str) -> MockStats {
        MockStats {
            row_count: 10,
            null_count: Some(0),
            min: Some(Datum::String(min.to_string())),
            max: Some(Datum::String(max.to_string())),
        }
    }

    fn run(op: PredicateOperator, lit: &str, stats: &MockStats) -> bool {
        let dt = varchar();
        data_leaf_may_match(
            0,
            &dt,
            &dt,
            op,
            &[Datum::String(lit.to_string())],
            stats,
        )
    }

    #[test]
    fn starts_with_prunes_when_max_below_pattern() {
        let stats = string_stats("aaa", "fooa");
        assert!(!run(PredicateOperator::StartsWith, "foob", &stats));
    }

    #[test]
    fn starts_with_prunes_when_min_past_pattern_range() {
        // [foo, fop) is the pat range. min "fop" is already past it.
        let stats = string_stats("fop", "zzz");
        assert!(!run(PredicateOperator::StartsWith, "foo", &stats));
    }

    #[test]
    fn starts_with_keeps_when_pattern_inside_range() {
        let stats = string_stats("aaa", "zzz");
        assert!(run(PredicateOperator::StartsWith, "foo", &stats));
    }

    #[test]
    fn starts_with_keeps_when_min_equals_pattern() {
        let stats = string_stats("foo", "foozzz");
        assert!(run(PredicateOperator::StartsWith, "foo", &stats));
    }

    #[test]
    fn starts_with_falls_open_when_stats_missing() {
        let stats = MockStats {
            row_count: 5,
            null_count: Some(0),
            min: None,
            max: None,
        };
        assert!(run(PredicateOperator::StartsWith, "foo", &stats));
    }

    #[test]
    fn ends_with_and_contains_fall_open() {
        let stats = string_stats("aaa", "zzz");
        assert!(run(PredicateOperator::EndsWith, "foo", &stats));
        assert!(run(PredicateOperator::Contains, "foo", &stats));
    }

    #[test]
    fn like_with_literal_prefix_prunes_like_starts_with() {
        // pattern "foo%" → prefix "foo"; max "fooa" already past pattern end?
        // No: max = "fooa" >= "foo" and min "aaa" < "fop". So this case keeps.
        let stats = string_stats("aaa", "fooa");
        assert!(run(PredicateOperator::Like, "foo%", &stats));
        // pattern "foo%": [foo, fop). file [zaa, zzz] — max < pat → prune.
        let stats = string_stats("zaa", "zzz");
        assert!(!run(PredicateOperator::Like, "foo%", &stats));
        // file [fop, zzz] — min already past prefix range → prune.
        let stats = string_stats("fop", "zzz");
        assert!(!run(PredicateOperator::Like, "foo%", &stats));
    }

    #[test]
    fn like_without_literal_prefix_falls_open() {
        let stats = string_stats("aaa", "ccc");
        // Leading wildcard → no prefix → fail open.
        assert!(run(PredicateOperator::Like, "%foo%", &stats));
        // Leading underscore → no prefix → fail open.
        assert!(run(PredicateOperator::Like, "_oo", &stats));
    }

    #[test]
    fn like_with_escaped_wildcard_in_prefix_is_decoded() {
        // "100\%foo" → literal prefix "100%foo".
        let stats = string_stats("100", "100%fzz");
        assert!(run(PredicateOperator::Like, r"100\%foo", &stats));
        let stats = string_stats("zzz0", "zzz9");
        assert!(!run(PredicateOperator::Like, r"100\%foo", &stats));
    }

    #[test]
    fn missing_field_returns_false_for_string_ops() {
        // Only IsNull is allowed when the field is missing.
        for op in [
            PredicateOperator::StartsWith,
            PredicateOperator::EndsWith,
            PredicateOperator::Contains,
            PredicateOperator::Like,
        ] {
            assert!(!missing_field_may_match(op, 5));
        }
    }

    // Sanity check: integer ops keep their existing semantics after the new
    // string variants are interleaved into the dispatcher.
    #[test]
    fn integer_eq_still_prunes_outside_range() {
        let dt = DataType::Int(IntType::new());
        let stats = MockStats {
            row_count: 10,
            null_count: Some(0),
            min: Some(Datum::Int(0)),
            max: Some(Datum::Int(100)),
        };
        assert!(!data_leaf_may_match(
            0,
            &dt,
            &dt,
            PredicateOperator::Eq,
            &[Datum::Int(500)],
            &stats,
        ));
    }

}
