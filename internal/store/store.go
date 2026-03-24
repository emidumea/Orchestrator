package store

import (
	"encoding/json"
	"fmt"

	"orchestrator/internal/models"

	"go.etcd.io/bbolt"
)

var taskBucket = []byte("tasks")

type Store struct {
	db *bbolt.DB
}

func CreateStore(dbPath string) (*Store, error) {
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to open the bbolt database: %v", err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(taskBucket)
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("Failed to create the task bucket: %v", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) SaveTask(task models.Task) error {
	jsonTask, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("An error occured while creating the JSON with the task data: %v", err)
	}

	err = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(taskBucket)

		return b.Put([]byte(task.ID), jsonTask)
	})

	return err
}

func (s *Store) GetTask(taskID string) (*models.Task, error) {
	var task models.Task

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(taskBucket)
		taskData := b.Get([]byte(taskID))

		if taskData == nil {
			return fmt.Errorf("Task with ID %s was not found", taskID)
		}

		return json.Unmarshal(taskData, &task)
	})

	if err != nil {
		return nil, err
	}

	return &task, nil

}

func (s *Store) ListTasks() ([]models.Task, error) {
	var tasks []models.Task

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(taskBucket)

		return b.ForEach(func (k, v []byte) error {
			var task models.Task

			err := json.Unmarshal(v, &task)
			if err != nil {
				return fmt.Errorf("An error occured while extracting the task data: %v", err)
			}
			tasks = append(tasks, task)

			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return tasks, nil
}