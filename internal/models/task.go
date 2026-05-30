package models

type TaskState string

const (
	Pending TaskState = "PENDING"
	Scheduled TaskState = "SCHEDULED"
	Running TaskState = "RUNNING"
	Completed TaskState = "COMPLETED"
	Failed TaskState = "FAILED"
)
type Task struct {
	ID string
	Image string
	ContainerID string
	ContainerName string
	WorkerID string
	State TaskState
	SubmittedAt int64 `json:"submitted_at"`
	ScheduledAt int64 `json:"scheduled_at"`
	StartedAt int64 `json:"started_at"`
}