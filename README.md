protoc --proto_path=$SRC_DIR --python_out=osmpq/protos --pyi_out osmpq/protos protos/fileformat.proto
protoc --proto_path=$SRC_DIR --python_out=osmpq/protos --pyi_out osmpq/protos protos/osmformat.proto
