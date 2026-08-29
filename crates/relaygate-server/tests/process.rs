use std::{
    error::Error,
    io,
    net::TcpListener,
    process::{Child, Command, ExitStatus, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const STARTUP_DEADLINE: Duration = Duration::from_secs(5);
#[cfg(unix)]
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[cfg(unix)]
#[test]
fn server_boots_health_checks_and_exits_on_sigterm() -> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let mut server = ChildGuard::spawn(
        server_command()
            .env("RELAYGATE_BIND_ADDR", &address)
            .env("RELAYGATE_CLIENT_KEYS", "echo.alpha=test-key"),
    )?;

    wait_until_healthy(&address, &mut server)?;

    let signal_status = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()?;
    assert!(signal_status.success(), "failed to send SIGTERM to server");

    let exit_status = server.wait_until(SHUTDOWN_DEADLINE)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "server did not exit before the shutdown deadline",
        )
    })?;
    assert!(
        exit_status.success(),
        "server exited unsuccessfully after SIGTERM: {exit_status}"
    );
    Ok(())
}

#[test]
fn invalid_cli_arguments_fail_with_actionable_errors() -> Result<(), Box<dyn Error>> {
    assert_failure(&["unknown"], "unknown command")?;
    assert_failure(&["check"], "usage: relaygate-server check <address>")?;
    assert_failure(
        &["check", "127.0.0.1:1", "extra"],
        "usage: relaygate-server check <address>",
    )?;
    Ok(())
}

#[test]
fn invalid_environment_configuration_fails_before_serving() -> Result<(), Box<dyn Error>> {
    let malformed_keys = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_CLIENT_KEYS", "echo.alpha")
        .output()?;
    assert_unsuccessful_output(
        &malformed_keys,
        "RELAYGATE_CLIENT_KEYS entries must use ClientId=ClientKey",
    );

    let zero_capacity = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_WRITER_QUEUE_CAPACITY", "0")
        .output()?;
    assert_unsuccessful_output(
        &zero_capacity,
        "RELAYGATE_WRITER_QUEUE_CAPACITY must be greater than zero",
    );
    Ok(())
}

fn server_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_relaygate-server"));
    for name in [
        "RELAYGATE_BIND_ADDR",
        "RELAYGATE_CLIENT_KEYS",
        "RELAYGATE_LOG",
        "RELAYGATE_MAX_BINDINGS",
        "RELAYGATE_MAX_FRAME_LEN",
        "RELAYGATE_MAX_LIVE_PIPES",
        "RELAYGATE_MAX_PENDING_OFFERS",
        "RELAYGATE_MAX_SESSIONS",
        "RELAYGATE_OFFER_TIMEOUT_MS",
        "RELAYGATE_WRITER_QUEUE_CAPACITY",
    ] {
        command.env_remove(name);
    }
    command.env("RELAYGATE_LOG", "warn");
    command
}

fn assert_failure(arguments: &[&str], expected: &str) -> Result<(), Box<dyn Error>> {
    let output = server_command().args(arguments).output()?;
    assert_unsuccessful_output(&output, expected);
    Ok(())
}

fn assert_unsuccessful_output(output: &Output, expected: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded; stderr: {stderr}"
    );
    assert!(
        stderr.contains(expected),
        "stderr did not contain {expected:?}; stderr: {stderr}"
    );
}

fn unused_loopback_address() -> io::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address.to_string())
}

fn wait_until_healthy(address: &str, server: &mut ChildGuard) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + STARTUP_DEADLINE;

    loop {
        if let Some(status) = server.try_wait()? {
            return Err(io::Error::other(format!(
                "server exited before becoming healthy: {status}"
            ))
            .into());
        }

        let check = server_command().args(["check", address]).output()?;
        if check.status.success() {
            return Ok(());
        }

        if Instant::now() >= deadline {
            let last_check = String::from_utf8_lossy(&check.stderr);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "server did not become healthy before the startup deadline; last check: {last_check}"
                ),
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn(command: &mut Command) -> io::Result<Self> {
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self { child })
    }

    #[cfg(unix)]
    fn id(&self) -> u32 {
        self.child.id()
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    #[cfg(unix)]
    fn wait_until(&mut self, timeout: Duration) -> io::Result<Option<ExitStatus>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(POLL_INTERVAL);
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
