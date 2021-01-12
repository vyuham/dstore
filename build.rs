fn main() {
    tonic_build::compile_protos("proto/dstore.proto").unwrap();
}