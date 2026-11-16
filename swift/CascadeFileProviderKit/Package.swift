// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "CascadeFileProviderKit",
    platforms: [.macOS(.v26)],
    products: [
        .library(name: "CascadeFileProviderKit", targets: ["CascadeFileProviderKit"]),
    ],
    targets: [
        .target(name: "CascadeFileProviderKit"),
        .testTarget(
            name: "CascadeFileProviderKitTests",
            dependencies: ["CascadeFileProviderKit"]
        ),
    ]
)
