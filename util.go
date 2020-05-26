package sladder

func rangeOverStringSortedSet(left, right []string, leftFn, rightFn, innerFn func(*string) bool) {
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
