package membership

// Member is one member of the current Raft configuration.
type Member struct {
	NodeID   string `json:"node_id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}

// Result is the current Raft configuration after an operator request.
// Changed is true only when that request committed a membership mutation.
type Result struct {
	Changed bool     `json:"changed"`
	Members []Member `json:"members"`
}
