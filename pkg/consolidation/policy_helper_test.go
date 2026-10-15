package consolidation

// id is the policy identity (test helper: sessions compute it once).
func (p Policy) id() []byte {
	var ops int
	order, _ := p.analyze(&ops)
	return p.idFor(order)
}

// verificationCounters keeps the authentication instrumentation (1 quorum / m bodies / L leaves).
func verificationCounters(c Counters) Counters {
	return Counters{QuorumChecks: c.QuorumChecks, BodyChecks: c.BodyChecks, LeafChecks: c.LeafChecks, EvidenceQuorumChecks: c.EvidenceQuorumChecks}
}
