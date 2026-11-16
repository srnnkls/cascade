import FileProvider
import Foundation
import Testing

@testable import CascadeFileProviderKit

private func identifiers(_ uids: [String]) -> [NSFileProviderItemIdentifier] {
    uids.map { $0 == "root" ? .rootContainer : NSFileProviderItemIdentifier($0) }
}

/// Pages of `uids` under the daemon's page tokens `after-<last uid>`.
private func pagedReply(_ pages: [[String]], parentUID: String, page: String?) -> FakeReply {
    let index = page.flatMap { token in pages.firstIndex { "after-\($0.last ?? "")" == token }.map { $0 + 1 } } ?? 0
    guard page == nil || index > 0, let uids = pages[safe: index] else {
        return .error(code: "pageExpired")
    }
    let next: Any = index + 1 < pages.count ? "after-\(uids.last ?? "")" : NSNull()
    return .result([
        "items": uids.map { wireItem(uid: $0, parentUID: parentUID, name: "f\($0)") },
        "next_page": next,
    ])
}

extension Array {
    subscript(safe index: Int) -> Element? {
        indices.contains(index) ? self[index] : nil
    }
}

@Test(.timeLimit(.minutes(1)))
func aContainerEnumeratorPagesToTheEndListingEachItemOnce() async throws {
    let pages = [["1", "2"], ["3", "4"], ["5"]]
    let daemon = try FakeDaemon { request in
        guard request.method == "list" else { return .error(code: "featureUnsupported") }
        return pagedReply(pages, parentUID: "7", page: request.params["page"] as? String)
    }
    defer { daemon.stop() }
    let enumerator = CascadeEnumerator(
        client: DaemonClient(socketPath: daemon.socketPath),
        enumeratedItemIdentifier: NSFileProviderItemIdentifier("7")
    )

    let listed = try await enumerateAll(enumerator)

    #expect(listed == identifiers(["1", "2", "3", "4", "5"]))
    #expect(daemon.requests.map(\.method) == ["list", "list", "list"])
    #expect(daemon.requests.allSatisfy { $0.params["uid"] as? String == "7" })
    #expect(daemon.requests.map { $0.params["page"] as? String } == [nil, "after-2", "after-4"])

    let root = CascadeEnumerator(
        client: DaemonClient(socketPath: daemon.socketPath), enumeratedItemIdentifier: .rootContainer
    )
    let firstRootPage = try await enumeratePage(
        root, from: NSFileProviderPage(NSFileProviderPage.initialPageSortedByDate as Data)
    ).get()
    #expect(firstRootPage.identifiers == identifiers(["1", "2"]))
    let rootList = try #require(daemon.requests.last)
    #expect(rootList.params["uid"] as? String == "root")
    #expect(rootList.params["page"] as? String == nil, "an initial page by date starts at the first page")
}

@Test(.timeLimit(.minutes(1)))
func theWorkingSetEnumeratorPagesThroughMaterialisedAndPinnedItems() async throws {
    let pages = [["11", "12", "13"], ["14"]]
    let daemon = try FakeDaemon { request in
        guard request.method == "workingSet" else { return .error(code: "featureUnsupported") }
        return pagedReply(pages, parentUID: "root", page: request.params["page"] as? String)
    }
    defer { daemon.stop() }
    let enumerator = CascadeEnumerator(
        client: DaemonClient(socketPath: daemon.socketPath), enumeratedItemIdentifier: .workingSet
    )

    let listed = try await enumerateAll(enumerator)

    #expect(listed == identifiers(["11", "12", "13", "14"]))
    #expect(daemon.requests.map(\.method) == ["workingSet", "workingSet"])
    #expect(daemon.requests.map { $0.params["page"] as? String } == [nil, "after-13"])
}

@Test(.timeLimit(.minutes(1)))
func aPageOver500BytesOrNotUTF8IsExpired() async throws {
    let daemon = try FakeDaemon { _ in .result(["items": [Any](), "next_page": NSNull()]) }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)
    let container = CascadeEnumerator(client: client, enumeratedItemIdentifier: NSFileProviderItemIdentifier("7"))
    let workingSet = CascadeEnumerator(client: client, enumeratedItemIdentifier: .workingSet)
    let oversized = NSFileProviderPage(Data(String(repeating: "p", count: 501).utf8))
    let notUTF8 = NSFileProviderPage(Data([0xFF, 0xFE, 0x80]))

    for (enumerator, page, label) in [
        (container, oversized, "container, 501 bytes"),
        (container, notUTF8, "container, not UTF-8"),
        (workingSet, oversized, "working set, 501 bytes"),
        (workingSet, notUTF8, "working set, not UTF-8"),
    ] {
        let result = await enumeratePage(enumerator, from: page)
        switch result {
        case .success:
            Issue.record("\(label): enumerated instead of pageExpired")
        case let .failure(error):
            #expect(isFileProviderError(error as NSError, .pageExpired), "\(label): \(error)")
        }
    }

    let limit = String(repeating: "q", count: 500)
    _ = try await enumeratePage(container, from: NSFileProviderPage(Data(limit.utf8))).get()
    #expect(daemon.requests.map { $0.params["page"] as? String } == [limit], "a 500-byte page is the daemon's")
}

@Test(.timeLimit(.minutes(1)))
func enumerateChangesDeliversUpdatesAndDeletionsAndFinishesWithTheDaemonsAnchor() async throws {
    let daemon = try FakeDaemon { request in
        switch request.method {
        case "currentAnchor":
            .result(["anchor": "store-b:42"])
        case "changes":
            .result([
                "updated": [
                    wireItem(uid: "21", parentUID: "root", name: "new.txt"),
                    wireItem(uid: "22", parentUID: "7", name: "moved.txt"),
                ],
                "deleted": ["23", "24"],
                "anchor": "store-b:45",
            ])
        default:
            .error(code: "featureUnsupported")
        }
    }
    defer { daemon.stop() }
    let enumerator = CascadeEnumerator(
        client: DaemonClient(socketPath: daemon.socketPath), enumeratedItemIdentifier: .workingSet
    )

    let current = await withCheckedContinuation { continuation in
        enumerator.currentSyncAnchor { continuation.resume(returning: $0?.rawValue) }
    }
    #expect(current == Data("store-b:42".utf8))

    let changes = try await enumerateChanges(
        enumerator, from: NSFileProviderSyncAnchor(Data("store-b:42".utf8))
    ).get()

    #expect(changes.updated == identifiers(["21", "22"]))
    #expect(changes.deleted == identifiers(["23", "24"]))
    #expect(changes.anchor.rawValue == Data("store-b:45".utf8))
    #expect(changes.moreComing == false)
    let request = try #require(daemon.requests.last)
    #expect(request.method == "changes")
    #expect(request.params["anchor"] as? String == "store-b:42")
}

@Test(.timeLimit(.minutes(1)))
func anAnchorOver500BytesOrUndecodableExpiresWithoutARequestAndSoDoesTheDaemonsVerdict() async throws {
    let daemon = try FakeDaemon { request in
        if request.params["anchor"] as? String == "foreign-store:9" {
            return .error(code: "syncAnchorExpired")
        }
        return .result(["updated": [Any](), "deleted": [String](), "anchor": "store-b:1"])
    }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)
    let enumerator = CascadeEnumerator(client: client, enumeratedItemIdentifier: .workingSet)

    for (anchor, label) in [
        (NSFileProviderSyncAnchor(Data(String(repeating: "a", count: 501).utf8)), "501 bytes"),
        (NSFileProviderSyncAnchor(Data([0xC3, 0x28, 0xFF])), "not UTF-8"),
    ] {
        switch await enumerateChanges(enumerator, from: anchor) {
        case .success:
            Issue.record("\(label): enumerated instead of syncAnchorExpired")
        case let .failure(error):
            #expect(isFileProviderError(error as NSError, .syncAnchorExpired), "\(label): \(error)")
        }
        let direct = await thrown { _ = try await client.changes(from: anchor) }
        #expect(isFileProviderError(direct, .syncAnchorExpired), "\(label), client: \(String(describing: direct))")
    }
    #expect(daemon.requests.isEmpty, "an invalid anchor never reaches the daemon")

    let limit = String(repeating: "b", count: 500)
    _ = try await enumerateChanges(enumerator, from: NSFileProviderSyncAnchor(Data(limit.utf8))).get()
    #expect(daemon.requests.last?.params["anchor"] as? String == limit, "a 500-byte anchor is the daemon's")

    switch await enumerateChanges(enumerator, from: NSFileProviderSyncAnchor(Data("foreign-store:9".utf8))) {
    case .success:
        Issue.record("an anchor the daemon expired enumerated changes")
    case let .failure(error):
        #expect(isFileProviderError(error as NSError, .syncAnchorExpired), "\(error)")
    }
}

/// Lists `pages` of identifiers asynchronously, as the system's
/// materialised-items enumerator does.
private final class PagedEnumerator: NSObject, NSFileProviderEnumerator, @unchecked Sendable {
    private let pages: [[NSFileProviderItemIdentifier]]

    init(_ pages: [[NSFileProviderItemIdentifier]]) {
        self.pages = pages
    }

    func invalidate() {}

    func enumerateItems(for observer: any NSFileProviderEnumerationObserver, startingAt page: NSFileProviderPage) {
        let index = Int(String(decoding: page.rawValue, as: UTF8.self)) ?? 0
        let items = (pages[safe: index] ?? []).map(ListedItem.init)
        let next = index + 1 < pages.count ? NSFileProviderPage(Data("\(index + 1)".utf8)) : nil
        let delivery = Locked((observer, items))
        DispatchQueue.global().async {
            let (observer, items) = delivery.value
            observer.didEnumerate(items)
            observer.finishEnumerating(upTo: next)
        }
    }
}

@Test(.timeLimit(.minutes(1)))
func everyIdentifierOfAPagedMaterialisedEnumeratorReachesOneMaterializedSetRequest() async throws {
    let daemon = try FakeDaemon { _ in .result([:]) }
    defer { daemon.stop() }
    let client = DaemonClient(socketPath: daemon.socketPath)
    let materialised = PagedEnumerator([
        identifiers(["root", "a", "b"]),
        identifiers(["c", "d"]),
        identifiers(["e"]),
    ])

    try await client.reportMaterializedSet(from: materialised)

    #expect(daemon.requests.map(\.method) == ["materializedSet"])
    let uids = daemon.requests.first?.params["uids"] as? [String] ?? []
    #expect(uids.sorted() == ["a", "b", "c", "d", "e", "root"])
}

@Test
func theTrashContainerIsUnsupportedAndOtherContainersGetAnEnumerator() throws {
    let client = DaemonClient(socketPath: "/tmp/cfpk-absent.sock")

    let trash = #expect(throws: CocoaError.self) {
        try CascadeEnumerator.forContainer(.trashContainer, client: client)
    }
    #expect(trash?.code == .featureUnsupported)
    #expect(trash.map { ($0 as NSError).domain } == NSCocoaErrorDomain)
    _ = try CascadeEnumerator.forContainer(.rootContainer, client: client)
    _ = try CascadeEnumerator.forContainer(.workingSet, client: client)
}
