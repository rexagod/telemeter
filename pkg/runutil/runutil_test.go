// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package runutil

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

type testCloser struct {
	err error
}

func (c testCloser) Close() error {
	return c.err
}

func TestCloseWithErrCapture(t *testing.T) {
	for _, tcase := range []struct {
		err    error
		closer io.Closer

		expectedErrStr string
	}{
		{
			err:            nil,
			closer:         testCloser{err: nil},
			expectedErrStr: "",
		},
		{
			err:            errors.New("test"),
			closer:         testCloser{err: nil},
			expectedErrStr: "test",
		},
		{
			err:            nil,
			closer:         testCloser{err: errors.New("test")},
			expectedErrStr: "close: test",
		},
		{
			err:            errors.New("test"),
			closer:         testCloser{err: errors.New("test")},
			expectedErrStr: "2 errors: test; close: test",
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			ret := tcase.err
			CloseWithErrCapture(&ret, tcase.closer, "close")

			if tcase.expectedErrStr == "" {
				if ret != nil {
					t.Error("Expected error to be nil")
					t.Fail()
				}
			} else {
				if ret == nil {
					t.Error("Expected error to be not nil")
					t.Fail()
				}

				if tcase.expectedErrStr != ret.Error() {
					t.Errorf("%s != %s", tcase.expectedErrStr, ret.Error())
					t.Fail()
				}
			}

		}); !ok {
			return
		}
	}
}

type loggerCapturer struct {
	// WasCalled is true if the Log() function has been called.
	WasCalled bool
}

func (lc *loggerCapturer) Log(keyvals ...interface{}) error {
	lc.WasCalled = true
	return nil
}

type emulatedCloser struct {
	io.Reader

	calls int
}

func (e *emulatedCloser) Close() error {
	e.calls++
	if e.calls == 1 {
		return nil
	}
	if e.calls == 2 {
		return errors.Wrap(os.ErrClosed, "can even be a wrapped one")
	}
	return errors.New("something very bad happened")
}

// newEmulatedCloser returns a ReadCloser with a Close method
// that at first returns success but then returns that
// it has been closed already. After that, it returns that
// something very bad had happened.
func newEmulatedCloser(r io.Reader) io.ReadCloser {
	return &emulatedCloser{Reader: r}
}

func formatMessage(msgAndArgs []interface{}) string {
	if len(msgAndArgs) == 0 {
		return ""
	}

	if msg, ok := msgAndArgs[0].(string); ok {
		return fmt.Sprintf("\n\nmsg: "+msg, msgAndArgs[1:]...)
	}
	return ""
}

// Equals fails the test if exp is not equal to act.
func equals(t *testing.T, exp, act interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if !reflect.DeepEqual(exp, act) {
		t.Fatalf("\033[31m%s\n\nexp: %#v\n\ngot: %#v\033[39m\n", formatMessage(msgAndArgs), exp, act)
	}
}

func TestCloseMoreThanOnce(t *testing.T) {
	lc := &loggerCapturer{}
	r := newEmulatedCloser(strings.NewReader("somestring"))

	CloseWithLogOnErr(lc, r, "should not be called")
	CloseWithLogOnErr(lc, r, "should not be called")
	equals(t, false, lc.WasCalled)

	CloseWithLogOnErr(lc, r, "should be called")
	equals(t, true, lc.WasCalled)
}
