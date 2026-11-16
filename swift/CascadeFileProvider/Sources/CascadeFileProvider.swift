// SPDX-License-Identifier: Apache-2.0

import CascadeFileProviderKit
import FileProvider
import Foundation

/// The Cascade domain's replicated extension: an adapter from the system's
/// calls onto protocol v2 of the daemon behind `fp.sock` in the App Group
/// container.
final class CascadeFileProvider: NSObject, NSFileProviderReplicatedExtension, NSFileProviderPartialContentFetching,
    NSFileProviderCustomAction
{
    private let domain: NSFileProviderDomain
    private let client: DaemonClient

    init(domain: NSFileProviderDomain) {
        self.domain = domain
        let container = appGroupContainer() ?? FileManager.default.temporaryDirectory
        client = DaemonClient(socketPath: daemonSocket(in: container).path)
        super.init()
    }

    func invalidate() {}

    func item(
        for identifier: NSFileProviderItemIdentifier,
        request _: NSFileProviderRequest,
        completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void
    ) -> Progress {
        completeOnce({ completionHandler($0.value, $0.failure) }) { [client] in
            try await client.item(identifier)
        }
    }

    func enumerator(
        for containerItemIdentifier: NSFileProviderItemIdentifier,
        request _: NSFileProviderRequest
    ) throws -> NSFileProviderEnumerator {
        try CascadeEnumerator.forContainer(containerItemIdentifier, client: client)
    }

    // MARK: Contents

    func fetchContents(
        for itemIdentifier: NSFileProviderItemIdentifier,
        version requestedVersion: NSFileProviderItemVersion?,
        request _: NSFileProviderRequest,
        completionHandler: @escaping (URL?, NSFileProviderItem?, Error?) -> Void
    ) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        return completeOnce({ completionHandler($0.value?.0, $0.value?.1, $0.failure) }, progress: progress) {
            [client, domain] in
            try await client.fetch(
                itemIdentifier,
                requestedVersion: requestedVersion,
                into: temporaryDirectory(of: domain),
                progress: progress
            )
        }
    }

    func fetchPartialContents(
        for itemIdentifier: NSFileProviderItemIdentifier,
        version requestedVersion: NSFileProviderItemVersion,
        request _: NSFileProviderRequest,
        minimalRange requestedRange: NSRange,
        aligningTo alignment: Int,
        options: NSFileProviderFetchContentsOptions,
        completionHandler: @escaping (URL?, NSFileProviderItem?, NSRange, NSFileProviderMaterializationFlags, Error?)
            -> Void
    ) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        return completeOnce(
            { result in
                let contents = result.value
                completionHandler(
                    contents?.url, contents?.item, contents?.range ?? requestedRange, [], result.failure
                )
            },
            progress: progress
        ) { [client, domain] in
            try await client.fetchRange(
                itemIdentifier,
                requestedVersion: requestedVersion,
                strict: options.contains(.strictVersioning),
                range: requestedRange,
                alignment: alignment,
                into: temporaryDirectory(of: domain),
                progress: progress
            )
        }
    }

    // MARK: Mutations

    func createItem(
        basedOn itemTemplate: NSFileProviderItem,
        fields _: NSFileProviderItemFields,
        contents url: URL?,
        options: NSFileProviderCreateItemOptions,
        request _: NSFileProviderRequest,
        completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void
    ) -> Progress {
        let template = Unchecked(itemTemplate)
        let mayAlreadyExist = options.contains(.mayAlreadyExist)
        return completeOnce({ completionHandler($0.value, [], false, $0.failure) }) { [client] in
            try await client.create(template.value, contents: url, mayAlreadyExist: mayAlreadyExist)
        }
    }

    func modifyItem(
        _ item: NSFileProviderItem,
        baseVersion _: NSFileProviderItemVersion,
        changedFields: NSFileProviderItemFields,
        contents newContents: URL?,
        options _: NSFileProviderModifyItemOptions,
        request _: NSFileProviderRequest,
        completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void
    ) -> Progress {
        let edited = Unchecked(item)
        let contents = changedFields.contains(.contents) ? newContents : nil
        return completeOnce({ completionHandler($0.value, [], false, $0.failure) }) { [client] in
            try await client.modify(edited.value, changedFields: changedFields, contents: contents)
        }
    }

    func deleteItem(
        identifier: NSFileProviderItemIdentifier,
        baseVersion _: NSFileProviderItemVersion,
        options: NSFileProviderDeleteItemOptions,
        request _: NSFileProviderRequest,
        completionHandler: @escaping (Error?) -> Void
    ) -> Progress {
        let recursive = options.contains(.recursive)
        return completeOnce({ completionHandler($0.failure) }) { [client] in
            try await client.delete(identifier, recursive: recursive)
        }
    }

    // MARK: Materialised set and actions

    func materializedItemsDidChange(completionHandler: @escaping () -> Void) {
        completeOnce({ _ in completionHandler() }) { [client, domain] in
            guard let manager = NSFileProviderManager(for: domain) else { return }
            try await client.reportMaterializedSet(from: manager.enumeratorForMaterializedItems())
        }
    }

    func performAction(
        identifier actionIdentifier: NSFileProviderExtensionActionIdentifier,
        onItemsWithIdentifiers itemIdentifiers: [NSFileProviderItemIdentifier],
        completionHandler: @escaping (Error?) -> Void
    ) -> Progress {
        completeOnce({ completionHandler($0.failure) }) { [client, domain] in
            guard let manager = NSFileProviderManager(for: domain) else {
                throw NSFileProviderError(.providerNotFound)
            }
            try await CascadeFileProviderKit.performAction(
                actionIdentifier, on: itemIdentifiers, client: client, manager: DomainManager(manager)
            )
        }
    }
}

/// The directory the system takes fetched contents from.
private func temporaryDirectory(of domain: NSFileProviderDomain) throws -> URL {
    guard let manager = NSFileProviderManager(for: domain) else {
        throw NSFileProviderError(.providerNotFound)
    }
    return try manager.temporaryDirectoryURL()
}

private extension Result {
    var value: Success? {
        try? get()
    }

    var failure: Failure? {
        if case let .failure(error) = self { error } else { nil }
    }
}
