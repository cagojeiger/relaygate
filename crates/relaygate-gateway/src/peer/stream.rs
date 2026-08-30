#[cfg(test)]
use relaygate_protocol::ErrorCode;

use super::{
    error::PeerError,
    identity::{StreamEndpoint, StreamOwner},
};

#[derive(Debug, Clone)]
pub(crate) struct RelayStream {
    state: RelayStreamState,
    owner: Option<StreamOwner>,
    local_finished: bool,
    remote_finished: bool,
}

impl RelayStream {
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn opening() -> Self {
        Self {
            state: RelayStreamState::Opening,
            owner: None,
            local_finished: false,
            remote_finished: false,
        }
    }

    #[must_use]
    pub(crate) const fn owned_opening(owner: StreamOwner) -> Self {
        Self {
            state: RelayStreamState::Opening,
            owner: Some(owner),
            local_finished: false,
            remote_finished: false,
        }
    }

    #[must_use]
    pub(crate) const fn owner(&self) -> Option<StreamOwner> {
        self.owner
    }

    pub(crate) fn opened(&mut self) -> Result<(), PeerError> {
        match self.state {
            RelayStreamState::Opening => {
                self.state = RelayStreamState::Open;
                Ok(())
            }
            RelayStreamState::Open | RelayStreamState::Closed(_) => {
                Err(PeerError::FailedPrecondition("RelayStream is not opening"))
            }
        }
    }

    pub(crate) fn fin(&mut self, sender: StreamEndpoint) -> Result<(), PeerError> {
        self.ensure_open()?;
        match sender {
            StreamEndpoint::Dialer => self.local_finished = true,
            StreamEndpoint::Acceptor => self.remote_finished = true,
        }
        if self.local_finished && self.remote_finished {
            self.state = RelayStreamState::Closed(StreamTerminal::Closed);
        }
        Ok(())
    }

    pub(crate) fn data(&self, sender: StreamEndpoint) -> Result<(), PeerError> {
        self.ensure_open()?;
        let finished = match sender {
            StreamEndpoint::Dialer => self.local_finished,
            StreamEndpoint::Acceptor => self.remote_finished,
        };
        if finished {
            return Err(PeerError::Protocol("DATA is not valid after FIN"));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn close(&mut self) {
        self.state = StreamTerminal::Closed.into();
    }

    #[cfg(test)]
    pub(crate) fn reset(&mut self, code: ErrorCode) {
        self.state = StreamTerminal::Reset(code).into();
    }

    #[must_use]
    pub(crate) const fn is_closed(&self) -> bool {
        matches!(self.state, RelayStreamState::Closed(_))
    }

    #[must_use]
    pub(crate) const fn is_open(&self) -> bool {
        matches!(self.state, RelayStreamState::Open)
    }

    fn ensure_open(&self) -> Result<(), PeerError> {
        match self.state {
            RelayStreamState::Open => Ok(()),
            RelayStreamState::Opening => {
                Err(PeerError::FailedPrecondition("RelayStream is not open yet"))
            }
            RelayStreamState::Closed(_) => {
                Err(PeerError::FailedPrecondition("RelayStream is closed"))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayStreamState {
    Opening,
    Open,
    Closed(StreamTerminal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamTerminal {
    Closed,
    #[cfg(test)]
    Reset(ErrorCode),
}

impl From<StreamTerminal> for RelayStreamState {
    fn from(value: StreamTerminal) -> Self {
        Self::Closed(value)
    }
}
