use tokio_util::sync::CancellationToken;

pub(crate) struct RuntimeLifetime {
    cancel: CancellationToken,
}

impl RuntimeLifetime {
    pub(crate) fn new(cancel: CancellationToken) -> Self {
        Self { cancel }
    }
}

impl Drop for RuntimeLifetime {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
