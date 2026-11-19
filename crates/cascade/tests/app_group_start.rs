//! `cascade start --app-group` as a process: which File Provider protocol a
//! start serves, its PID file and socket across a stop, and two SMB backends
//! written by `init` and `backend-add`.
#![cfg(target_os = "macos")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::io::{Read as _, Write as _};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

const DEADLINE: Duration = Duration::from_secs(20);

struct Setup {
    config: TempDir,
    app_group: TempDir,
    mount: TempDir,
    share: TempDir,
}

impl Setup {
    /// A config directory with one local backend `nas` holding `hello.txt`.
    fn new() -> Self {
        let setup = Self {
            config: tempfile::tempdir().unwrap(),
            app_group: tempfile::tempdir().unwrap(),
            mount: tempfile::tempdir().unwrap(),
            share: tempfile::tempdir().unwrap(),
        };
        std::fs::write(setup.share.path().join("hello.txt"), b"hello").unwrap();
        std::fs::write(
            setup.config.path().join("config.toml"),
            format!(
                "[backends.nas]\ntype = \"local\"\nmount = \"nas\"\n\n[mount]\npoint = \"{}\"\n",
                setup.mount.path().display()
            ),
        )
        .unwrap();
        std::fs::write(
            setup.config.path().join("nas.toml"),
            format!(
                "type = \"local\"\nid = \"nas\"\nroot_path = \"{}\"\n",
                setup.share.path().display()
            ),
        )
        .unwrap();
        setup
    }

    fn cascade(&self, app_group: Option<&Path>, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cascade"));
        command.arg("--config").arg(self.config.path());
        if let Some(group) = app_group {
            command.arg("--app-group").arg(group);
        }
        command
            .args(args)
            .env_remove("CASCADE_PRESENTER")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }

    fn spawn(&self, app_group: Option<&Path>, args: &[&str]) -> Daemon {
        Daemon(self.cascade(app_group, args).spawn().unwrap())
    }
}

/// A daemon process, killed if a test fails before stopping it.
struct Daemon(Child);

impl Daemon {
    fn pid(&self) -> u32 {
        self.0.id()
    }

    fn terminate(&mut self) -> ExitStatus {
        let pid = nix::unistd::Pid::from_raw(i32::try_from(self.pid()).unwrap());
        nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM).unwrap();
        self.wait()
    }

    fn wait(&mut self) -> ExitStatus {
        let started = Instant::now();
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(started.elapsed() < DEADLINE, "the daemon did not exit");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Sends one request and returns the whole response frame.
fn rpc(socket: &Path, method: &str, params: Value) -> Value {
    let mut stream = UnixStream::connect(socket).unwrap();
    stream.set_read_timeout(Some(DEADLINE)).unwrap();
    let body = serde_json::to_vec(&json!({"id": 1, "method": method, "params": params})).unwrap();
    stream
        .write_all(&u32::try_from(body.len()).unwrap().to_be_bytes())
        .unwrap();
    stream.write_all(&body).unwrap();
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len).unwrap();
    let mut response = vec![0_u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut response).unwrap();
    serde_json::from_slice(&response).unwrap()
}

/// Waits until `socket` accepts a connection while `daemon` keeps running.
fn await_socket(daemon: &mut Daemon, socket: &Path) {
    let started = Instant::now();
    while UnixStream::connect(socket).is_err() {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            panic!(
                "the daemon exited with {status} before serving {}",
                socket.display()
            );
        }
        assert!(
            started.elapsed() < DEADLINE,
            "{} never accepted a connection",
            socket.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn await_pid_file(pid_file: &Path, pid: u32) {
    let started = Instant::now();
    while std::fs::read_to_string(pid_file)
        .ok()
        .as_deref()
        .map(str::trim)
        != Some(pid.to_string().as_str())
    {
        assert!(
            started.elapsed() < DEADLINE,
            "{} never named PID {pid}",
            pid_file.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Whether the daemon behind `socket` answers protocol v2's `hello`.
fn serves_v2(socket: &Path) -> bool {
    rpc(socket, "hello", json!({"version": 2}))["result"]["version"] == 2
}

/// Whether the daemon behind `socket` answers protocol v1's `getItem` for
/// the root container.
fn serves_v1(socket: &Path) -> bool {
    rpc(socket, "getItem", json!({"id": "root"}))["result"]["item"]["id"] == "root"
}

fn names(items: &Value) -> Vec<String> {
    items
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["name"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn app_group_alone_serves_protocol_v2_and_sigterm_removes_socket_and_pid_file() {
    let setup = Setup::new();
    let group = setup.app_group.path();
    let socket = group.join("fp.sock");
    let pid_file = group.join("cascade.pid");
    let mut daemon = setup.spawn(Some(group), &["start", "--no-mount"]);

    await_socket(&mut daemon, &socket);
    assert!(
        serves_v2(&socket),
        "start --app-group did not serve protocol v2"
    );
    await_pid_file(&pid_file, daemon.pid());

    let status = daemon.terminate();

    assert!(status.success(), "the daemon exited with {status}");
    assert!(!socket.exists(), "fp.sock outlived the daemon");
    assert!(!pid_file.exists(), "cascade.pid outlived the daemon");
}

#[test]
fn app_group_naming_the_config_directory_serves_protocol_v2() {
    let setup = Setup::new();
    let group = setup.config.path();
    let socket = group.join("fp.sock");
    let mut daemon = setup.spawn(Some(group), &["start", "--no-mount"]);

    await_socket(&mut daemon, &socket);

    assert!(
        serves_v2(&socket),
        "start --app-group <config dir> did not serve protocol v2"
    );
    daemon.terminate();
}

#[test]
fn file_provider_without_app_group_serves_protocol_v1() {
    let setup = Setup::new();
    let socket = setup.config.path().join("fp.sock");
    let mut daemon = setup.spawn(None, &["start", "--file-provider", "--no-mount"]);

    await_socket(&mut daemon, &socket);

    assert!(
        serves_v1(&socket),
        "start --file-provider stopped serving protocol v1"
    );
    assert!(
        !serves_v2(&socket),
        "start --file-provider served protocol v2"
    );
    daemon.terminate();
}

#[test]
fn two_smb_backends_from_init_and_backend_add_are_both_served() {
    let setup = Setup::new();
    let group = setup.app_group.path();
    let mount: PathBuf = setup.mount.path().to_path_buf();
    let init = setup
        .cascade(
            None,
            &[
                "init",
                "--backend-type",
                "smb",
                "--name",
                "office",
                "--mount-point",
                mount.to_str().unwrap(),
                "--smb-host",
                "127.0.0.1",
                "--smb-share",
                "office",
                "--smb-port",
                "1",
                "--smb-username",
                "alice",
                "--smb-password",
                "secret",
            ],
        )
        .status()
        .unwrap();
    assert!(init.success(), "cascade init exited with {init}");

    let mut add = setup.cascade(None, &["backend-add", "smb", "--name", "media"]);
    add.stdin(Stdio::piped()).stderr(Stdio::piped());
    let mut adding = add.spawn().unwrap();
    adding
        .stdin
        .take()
        .unwrap()
        .write_all(b"127.0.0.1\nmedia\n1\nbob\nsecret\n")
        .unwrap();
    let added = adding.wait_with_output().unwrap();
    assert!(
        added.status.success(),
        "cascade backend-add exited with {}: {}",
        added.status,
        String::from_utf8_lossy(&added.stderr)
    );

    let socket = group.join("fp.sock");
    let mut daemon = setup.spawn(Some(group), &["start", "--no-mount"]);
    await_socket(&mut daemon, &socket);
    let root = rpc(&socket, "list", json!({"uid": "root"}));
    let mut mounts = names(&root["result"]["items"]);
    mounts.sort();
    daemon.terminate();

    assert_eq!(
        mounts.len(),
        2,
        "two SMB backends share one id; the domain root lists {mounts:?}"
    );
}

fn nas_names_with_mount_point_cascade(cascade: &str) -> Vec<String> {
    let setup = Setup::new();
    std::fs::create_dir(setup.share.path().join("Mutagen")).unwrap();
    std::fs::write(
        setup.share.path().join("Mutagen").join("inside.txt"),
        b"left to Mutagen",
    )
    .unwrap();
    std::fs::write(setup.mount.path().join(".cascade"), cascade).unwrap();
    let group = setup.app_group.path();
    let socket = group.join("fp.sock");
    let mut daemon = setup.spawn(Some(group), &["start", "--no-mount"]);
    await_socket(&mut daemon, &socket);

    let root = rpc(&socket, "list", json!({"uid": "root"}));
    let nas = root["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["name"] == "nas")
        .unwrap_or_else(|| panic!("the domain root lists no nas: {root}"))["uid"]
        .clone();
    let listed = rpc(&socket, "list", json!({"uid": nas}));
    daemon.terminate();
    let mut listed = names(&listed["result"]["items"]);
    listed.sort();
    listed
}

#[test]
fn a_folder_ignored_by_the_mount_point_cascade_is_absent_from_the_domain() {
    assert_eq!(
        nas_names_with_mount_point_cascade("Mutagen/\n"),
        ["hello.txt"]
    );
}

#[test]
fn a_mount_point_cascade_without_ignore_rules_leaves_the_listing_unchanged() {
    assert_eq!(
        nas_names_with_mount_point_cascade("# no ignore rules\n"),
        ["Mutagen", "hello.txt"]
    );
}
