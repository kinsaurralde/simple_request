load("@rules_java//java:defs.bzl", "java_import")

filegroup(
    name = "grpc_gcp_jar",
    srcs = ["grpc-gcp-java/grpc-gcp/build/libs/grpc-gcp-1.7.1-SNAPSHOT.jar"],
    visibility = ["//src/main/java/com/google/spanner/cloud/rain:__pkg__"],
)

java_import(
    name = "grpc_gcp_local",
    jars = [":grpc_gcp_jar"],
    visibility = ["//src/main/java/com/google/spanner/cloud/rain:__pkg__"],
)