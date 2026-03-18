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
}