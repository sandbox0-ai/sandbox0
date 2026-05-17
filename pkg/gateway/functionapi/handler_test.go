package functionapi

import "testing"

func TestTryLockFunctionRevisionPublishSerializesFunction(t *testing.T) {
	handler := &Handler{}

	release, ok := handler.tryLockFunctionRevisionPublish("team-a", "function-a")
	if !ok {
		t.Fatal("first publish lock attempt failed")
	}
	if secondRelease, ok := handler.tryLockFunctionRevisionPublish("team-a", "function-a"); ok {
		secondRelease()
		t.Fatal("second publish lock attempt for same function succeeded")
	}

	otherRelease, ok := handler.tryLockFunctionRevisionPublish("team-a", "function-b")
	if !ok {
		t.Fatal("publish lock for different function failed")
	}
	otherRelease()

	release()

	reacquired, ok := handler.tryLockFunctionRevisionPublish("team-a", "function-a")
	if !ok {
		t.Fatal("publish lock was not reusable after release")
	}
	reacquired()
}
