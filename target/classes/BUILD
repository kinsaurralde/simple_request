filegroup(
    name = "logging",
    srcs = [
        "logging.properties",
    ],
    visibility = [
        "//src/main/java/com/google/spanner/cloud/rain:__subpackages__",
        "//src/test/java/com/google/spanner/cloud/rain:__subpackages__",
    ],
)

exports_files([
    "META-INF/services/io.grpc.NameResolverProvider",
    "logging.properties",
])
