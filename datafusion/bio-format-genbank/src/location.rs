//! Convert INSDC locations into local bounds and ordered parts.

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Part {
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub reverse: bool,
}

#[derive(Default)]
pub(crate) struct Location {
    pub parts: Option<Vec<Part>>,
    pub bounds: Option<(i64, i64)>,
    pub reverse: Option<bool>,
}

enum ParseError {
    Invalid,
    Unsupported,
}

fn position(s: &str) -> Result<i64, ParseError> {
    let s = s.strip_prefix(['<', '>']).unwrap_or(s);
    if s.contains(['.', '(', ')', '?']) {
        return Err(ParseError::Unsupported);
    }
    s.parse::<i64>()
        .ok()
        .filter(|v| *v > 0)
        .ok_or(ParseError::Invalid)
}

fn parse(
    expr: &str,
    zero: bool,
    length: i64,
    circular: bool,
    depth: usize,
    parts: &mut Vec<Part>,
) -> Result<(), ParseError> {
    if depth > 64 || expr.is_empty() {
        return Err(ParseError::Invalid);
    }
    if let Some((op, args)) = expr.split_once('(') {
        if !matches!(op, "join" | "order" | "complement") {
            if op.contains(':')
                || matches!(op, "one-of" | "bond")
                || op.is_empty()
                || op.ends_with("..")
            {
                return Err(ParseError::Unsupported);
            }
            return Err(ParseError::Invalid);
        }
        let args = args.strip_suffix(')').ok_or(ParseError::Invalid)?;
        let mut level = 0usize;
        let mut start = 0;
        let first = parts.len();
        for (i, c) in args.char_indices() {
            match c {
                '(' => level += 1,
                ')' => level = level.checked_sub(1).ok_or(ParseError::Invalid)?,
                ',' if level == 0 => {
                    if op == "complement" {
                        return Err(ParseError::Invalid);
                    }
                    parse(&args[start..i], zero, length, circular, depth + 1, parts)?;
                    start = i + 1;
                }
                _ => {}
            }
        }
        if level != 0 {
            return Err(ParseError::Invalid);
        }
        parse(&args[start..], zero, length, circular, depth + 1, parts)?;
        if op == "complement" {
            parts[first..].reverse();
            for part in &mut parts[first..] {
                part.reverse = !part.reverse;
            }
        }
        return Ok(());
    }
    let (remote, local) = match expr.split_once(':') {
        Some((acc, s)) if !acc.is_empty() => (true, s),
        Some(_) => return Err(ParseError::Invalid),
        None => (false, expr),
    };
    let bounds = if let Some((a, b)) = local.split_once('^') {
        let (a, b) = (position(a)?, position(b)?);
        if a.checked_add(1) != Some(b) && !(circular && a == length && b == 1) {
            return Err(ParseError::Invalid);
        }
        // A site contains no base. Only half-open coordinates can express it.
        zero.then_some((a, a))
    } else {
        let (a, b) = local.split_once("..").unwrap_or((local, local));
        let (a, b) = (position(a)?, position(b)?);
        if b < a {
            return Err(ParseError::Invalid);
        }
        Some((a - i64::from(zero), b))
    };
    let (start, end) = bounds.filter(|_| !remote).unzip();
    parts.push(Part {
        start,
        end,
        reverse: false,
    });
    Ok(())
}

pub(crate) fn decode(
    expr: &str,
    zero: bool,
    circular: bool,
    length: i64,
) -> Result<Location, String> {
    let compact;
    let expr = if expr.contains(char::is_whitespace) {
        compact = expr
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>();
        compact.as_str()
    } else {
        expr
    };
    let mut parts = Vec::new();
    match parse(expr, zero, length, circular, 0, &mut parts) {
        Err(ParseError::Unsupported) => return Ok(Location::default()),
        Err(ParseError::Invalid) => return Err(format!("invalid location {expr:?}")),
        Ok(()) => {}
    }
    let reverse = parts
        .first()
        .map(|p| p.reverse)
        .filter(|&s| parts.iter().all(|p| p.reverse == s));
    let wraps = circular
        && parts.windows(2).any(|p| match (p[0].start, p[1].start) {
            (Some(a), Some(b)) if p[0].reverse == p[1].reverse => {
                if p[0].reverse {
                    a < b
                } else {
                    b < a
                }
            }
            _ => false,
        });
    let bounds = if !wraps && parts.iter().all(|p| p.start.is_some()) {
        parts
            .iter()
            .filter_map(|p| p.start)
            .min()
            .zip(parts.iter().filter_map(|p| p.end).max())
    } else {
        None
    };
    Ok(Location {
        parts: Some(parts),
        bounds,
        reverse,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_origin_and_mixed_remote_locations() {
        let result = decode("complement(join(90..100, 1..10))", true, true, 100).unwrap();
        assert_eq!(result.bounds, None);
        let parts = result.parts.unwrap();
        assert_eq!((parts[0].start, parts[1].start), (Some(0), Some(89)));
        let result = decode("order(10..20,AB123.1:3..6)", false, false, 100).unwrap();
        assert_eq!(result.bounds, None);
        assert_eq!(result.parts.unwrap()[1].start, None);
        let result = decode("join(1..4,complement(10..20))", true, false, 100).unwrap();
        assert_eq!(result.bounds, Some((0, 20)));
        assert_eq!(result.reverse, None);
    }

    #[test]
    fn points_sites_and_invalid_locations() {
        assert_eq!(decode("1", true, false, 100).unwrap().bounds, Some((0, 1)));
        assert_eq!(
            decode("100^1", true, true, 100).unwrap().bounds,
            Some((100, 100))
        );
        assert!(
            decode("one-of(1,2)", true, false, 100)
                .unwrap()
                .parts
                .is_none()
        );
        for expr in ["0..3", "5..2", "join(1..3,)", "complement(1..3", "4^9"] {
            assert!(decode(expr, true, false, 100).is_err(), "{expr}");
        }
    }
}
