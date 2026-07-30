package runtimewatch

import "time"

func deadlineNow() time.Time {
	return time.Now().Add(time.Second)
}
