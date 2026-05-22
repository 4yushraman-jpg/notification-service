package mailer

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsTransientSMTPError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"421", fmt.Errorf("smtp 421 service not available"), true},
		{"450", fmt.Errorf("450 mailbox busy"), true},
		{"451", fmt.Errorf("451 temporary local problem"), true},
		{"452", fmt.Errorf("452 insufficient storage"), true},
		{"throttle", errors.New("SMTP throttling: too many emails per second"), true},
		{"timeout", errors.New("i/o timeout"), true},
		{"reset", errors.New("connection reset by peer"), true},
		{"permanent 550 mailbox", fmt.Errorf("550 mailbox not found"), false},
		{"permanent 553", fmt.Errorf("553 invalid recipient"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsTransientSMTPError(tc.err); got != tc.want {
				t.Fatalf("IsTransientSMTPError() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsPermanentSMTPError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"550 mailbox", fmt.Errorf("550 mailbox unavailable"), true},
		{"550 generic only", fmt.Errorf("550 policy rejection"), false},
		{"551", fmt.Errorf("551 user not local"), true},
		{"553", fmt.Errorf("553 invalid domain"), true},
		{"554", fmt.Errorf("554 transaction failed"), true},
		{"invalid recipient", errors.New("invalid recipient address"), true},
		{"transient 421", fmt.Errorf("421 try again"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPermanentSMTPError(tc.err); got != tc.want {
				t.Fatalf("IsPermanentSMTPError() = %v, want %v", got, tc.want)
			}
		})
	}
}
