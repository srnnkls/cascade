// SPDX-License-Identifier: Apache-2.0

import SwiftUI

/// The host app runs as a menu-bar agent: it embeds the extension and the
/// `cascade` daemon and keeps both connected for as long as the user is
/// logged in.
@main
struct CascadeFileProviderHostApp: App {
    @NSApplicationDelegateAdaptor(HostAgent.self) private var agent

    var body: some Scene {
        MenuBarExtra("Cascade", systemImage: "externaldrive.connected.to.line.below") {
            ContentView(agent: agent)
        }
    }
}
