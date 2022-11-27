package messagewriter

import (
	"testing"
)

func TestProtoBin2Json(t *testing.T) {

	result, err := protoBin2Json(nil, "../../data/protomodel/user.desc", "protomodel.User")
	if err != nil {
		t.Fatal(err)
	}

	if string(result) != "{}" {
		t.Fatal("result should return empty json message for nil")
	}
}
