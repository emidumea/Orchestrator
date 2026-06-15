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
	ID string `json:"ID"`
	Image string `json:"image"`
	Command []string `json:"command,omitempty"`
	ContainerID string `json:"ContainerID"`
	ContainerName string `json:"ContainerName"`
	WorkerID string `json:"WorkerID"`
	State TaskState `json:"State"`
	SubmittedAt int64 `json:"submitted_at"`
	ScheduledAt int64 `json:"scheduled_at"`
	StartedAt int64 `json:"started_at"`
}