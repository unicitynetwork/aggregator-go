package models

const (
	FinalityStatusFinalized  = "finalized"
	FinalityStatusFinalizing = "finalizing"
	FinalityStatusProposed   = "proposed"
	FinalityStatusAbandoned  = "abandoned"
)

func normalizeFinalityStatus(status string) string {
	if status == "" {
		return FinalityStatusFinalized
	}
	return status
}

func normalizeBlockFinalityStatus(status string, finalized bool) string {
	if status != "" {
		return status
	}
	if finalized {
		return FinalityStatusFinalized
	}
	return FinalityStatusFinalizing
}
