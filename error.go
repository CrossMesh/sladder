package sladder

// Errors contains a set of errors.
type Errors []error

func (e Errors) Error() (s string) {
	if len(e) < 1 {
		return
	}

	for _, err := range e[:len(e)-1] {
		s += err.Error() + "=>"
	}
	s += e[len(e)-1].Error()

	return
}

// AsError presents itself as a normal error.
func (e Errors) AsError() error {
	switch {
	case len(e) < 1:
		return nil
	case len(e) == 1:
		return e[0]
	}
	return e
}
