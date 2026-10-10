package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strconv"
)

// StepID: lowercase hex of the first 16 bytes of SHA-256(name + "\x00" + decimal occurrence),
// occurrence counting from 0.
func StepID(name string, occurrence int) string {
	sum := sha256.Sum256([]byte(name + "\x00" + strconv.Itoa(occurrence)))
	return hex.EncodeToString(sum[:16])
}

var (
	stepIDRe       = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	workflowNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	uuidRe         = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	invocationIDRe = regexp.MustCompile(`^inv_[a-z2-7]{26}$`)
	payloadIDRe    = regexp.MustCompile(`^pay_[a-z2-7]{26}$`)
	queueRe        = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	workerIDRe     = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	errorCodeRe    = regexp.MustCompile(`^(user|workflows\.[a-z_]+)$`)
)

// ValidStepIDOverride: an explicit step ID matches ^[A-Za-z0-9._:-]{1,128}$.
func ValidStepIDOverride(s string) bool { return stepIDRe.MatchString(s) }

// ValidWorkflowName: ^[A-Za-z0-9_-]{1,64}$.
func ValidWorkflowName(s string) bool { return workflowNameRe.MatchString(s) }

// ValidQueue: a pull queue name, ^[a-z0-9][a-z0-9-]{0,62}$.
func ValidQueue(s string) bool { return queueRe.MatchString(s) }

// ValidWorkerID: ^[A-Za-z0-9._:-]{1,128}$.
func ValidWorkerID(s string) bool { return workerIDRe.MatchString(s) }
