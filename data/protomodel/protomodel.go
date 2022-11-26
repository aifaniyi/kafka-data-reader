package protomodel

//go:generate sh -c "rm *.pb.go ; protoc --go_out=../protomodel --proto_path=../protomodel/protobuf ../protomodel/protobuf/*.proto"
//go:generate sh -c "rm *.desc ; protoc --descriptor_set_out=user.desc --include_imports --proto_path=../protomodel/protobuf ../protomodel/protobuf/*.proto"
