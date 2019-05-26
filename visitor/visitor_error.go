package visitor

import "fmt"

type VisitorError struct {
	msg string
}

func NewVisitorError(msg string) *VisitorError {
	return &VisitorError{
		msg: fmt.Sprintf("VisitorError: %s", msg),
	}
}

func (this *VisitorError) Error() string {
	return this.msg
}
