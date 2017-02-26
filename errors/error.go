package errors

import "fmt"

type Kind uint8

const (
	Other Kind = iota
	Invalid
	IO
)

func (k Kind) String() string {
	switch k {
	case Other:
		return "other"
	case Invalid:
		return "invalid operation"
	case IO:
		return "I/O error"
	default:
		return "invalid kind"
	}
}

type Error struct {
	Kind Kind
	Op   string
	Err  error
}

func (e Error) Error() string {
	return fmt.Sprintf("{kind: %s op: %q err: %v}", e.Kind, e.Op, e.Err)
}

func E(kind Kind, op string, err error) error {
	return &Error{
		Kind: kind,
		Op:   op,
		Err:  err,
	}
}

type errorString struct {
	s string
}

func (e *errorString) Error() string { return e.s }

func Str(s string) error {
	return &errorString{s}
}

func Errorf(format string, args ...interface{}) error {
	return Str(fmt.Sprintf(format, args...))
}
