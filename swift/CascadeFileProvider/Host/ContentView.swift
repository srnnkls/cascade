// SPDX-License-Identifier: Apache-2.0

import SwiftUI

/// The agent's menu: its status and Quit.
struct ContentView: View {
    @ObservedObject var agent: HostAgent

    var body: some View {
        Text(agent.status)
        Divider()
        Button("Quit Cascade") {
            NSApplication.shared.terminate(nil)
        }
        .keyboardShortcut("q")
    }
}
