package messagewriter

import (
	"os"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func protoBin2Json(data []byte, descriptorFile, descriptorFullname string) (string, error) {
	descriptorFullnameSplit := strings.Split(descriptorFullname, ".")
	descriptorName := descriptorFullnameSplit[len(descriptorFullnameSplit)-1]

	registry, err := createProtoRegistry(descriptorFile)
	if err != nil {
		return "", err
	}

	descByName, err := registry.FindDescriptorByName(protoreflect.FullName(descriptorFullname))
	if err != nil {
		return "", err
	}

	msgTypeFromFileDesc := descByName.
		ParentFile().
		Messages().
		ByName(protoreflect.Name(descriptorName))

	msg := dynamicpb.NewMessage(msgTypeFromFileDesc)
	err = proto.Unmarshal(data, msg)
	if err != nil {
		return "", err
	}

	jsonBytes, err := protojson.Marshal(msg)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

func createProtoRegistry(filename string) (*protoregistry.Files, error) {
	descriptorContent, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	descriptorSet := descriptorpb.FileDescriptorSet{}

	err = proto.Unmarshal(descriptorContent, &descriptorSet)
	if err != nil {
		return nil, err
	}

	return protodesc.NewFiles(&descriptorSet)
}
