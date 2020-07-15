package util

import "sort"

// RangeOverStringSortedSet ranges over two sorted string set.
func RangeOverStringSortedSet(left, right []string, leftFn, rightFn, innerFn func(*string) bool) {
	var last *string
	ll, rl := len(left), len(right)
	l, r := 0, 0

	emit := func(s *string, fn func(*string) bool) bool {
		if last == nil || *last != *s {
			last = s
			if fn != nil {
				return fn(s)
			}
		}
		return true
	}

	for cont := true; cont; {
		if l >= ll {
			if r >= rl { // reach the end.
				break
			}
			cont = emit(&right[r], rightFn) // belong to right set.
			r++
		} else {
			if r >= rl {
				cont = emit(&left[l], leftFn) // belong to left set.
				l++
			} else {
				// belong to both.
				if left[l] == right[r] {
					cont = emit(&left[l], innerFn)
					l++
				} else if left[l] < right[r] {
					cont = emit(&left[l], leftFn)
					l++
				} else {
					cont = emit(&right[r], rightFn)
					r++
				}
			}
		}
	}
}

// MergeStringSortedSet merges the right to the left.
func MergeStringSortedSet(left, right []string) []string {
	oriLeft := left
	left = append(left, right...)
	if len(oriLeft) > 0 {
		for l, r := len(left), len(right); r > 0; l-- {
			if l < 1 || left[l-1] < right[r-1] {
				left[l-1] = right[r-1] // right
				r--
			}
			l--
		}
	}
	return left
}

// RemoveStringSortedSet removes specific values from the set.
func RemoveStringSortedSet(s []string, rs ...string) []string {
	if len(s) < 1 || len(rs) < 1 {
		return s
	}
	sort.Strings(rs)
	p := 0
	for b, r := 0, 0; b < len(s); {
		if r < len(rs) {
			if s[b] < rs[r] { // accept
				if p != b {
					s[p] = s[b]
				}
				p++
				b++
			} else if s[b] > rs[r] {
				r++
			} else { // s[b] == rs[r]
				b++ // remove
			}
		} else if p == b {
			// fast path: nothing removed. stop now.
			b = len(s)
			p = b
		} else {
			// compact.
			s[p] = s[b]
			p++
			b++
		}
	}
	return s[:p]
}

// AddStringSortedSet add specific values to the set.
func AddStringSortedSet(s []string, rs ...string) []string {
	sort.Strings(rs)
	RangeOverStringSortedSet(s, rs, nil, func(ns *string) bool {
		s = append(s, *ns)
		return true
	}, nil)
	return s
}
