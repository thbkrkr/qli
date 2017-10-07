package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"os/exec"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type TasksExecutor struct {
	name  string
	tasks map[string]*Task
	lock  sync.RWMutex
	logs  chan<- []byte
	total int
}

func NewTaskExecutor(name string, logs chan []byte) *TasksExecutor {
	return &TasksExecutor{
		name:  name,
		logs:  logs,
		tasks: map[string]*Task{},
		lock:  sync.RWMutex{},
	}
}

func (e *TasksExecutor) exec(args []string) (string, error) {
	if len(args) == 0 {
		return "ping!", nil
	}
	go e.startTask(args)
	return "", nil
}

func (e *TasksExecutor) startTask(args []string) {
	taskID := fmt.Sprintf("%d", e.total)
	e.total++
	cmdArgs := strings.Join(args, " ")
	cmd := exec.Command(args[0], args[1:]...)

	task := &Task{
		ID:      taskID,
		Command: cmdArgs,
		State:   "running",
		Cmd:     cmd,
	}

	e.lock.Lock()
	e.tasks[taskID] = task
	e.lock.Unlock()

	// Display the command to execute in a <pre> with the task id
	e.logs <- displayCmd(taskID, e.name, cmdArgs)

	stdoutReader, err := cmd.StdoutPipe()
	if err != nil {
		log.WithField("cmd", args[0]).WithError(err).Error("Error creating stdout pipe")
		e.logs <- displayLine(taskID, e.name, err.Error())
		return
	}
	stderrReader, err := cmd.StderrPipe()
	if err != nil {
		log.WithField("cmd", args[0]).WithError(err).Error("Error creating stderr pipe")
		e.logs <- displayLine(taskID, e.name, err.Error())
		return
	}

	// Stream command execution and attach stdin/stderr to the <pre> using the task id
	scanner := bufio.NewScanner(stdoutReader)
	go func() {
		for scanner.Scan() {
			e.logs <- displayLine(taskID, e.name, scanner.Text())
		}
	}()
	scannerStderr := bufio.NewScanner(stderrReader)
	go func() {
		for scannerStderr.Scan() {
			e.logs <- displayLine(taskID, e.name, scannerStderr.Text())
		}
	}()

	log.WithField("cmd", cmdArgs).Info("start task")

	err = cmd.Start()
	if err != nil {
		err2 := e.markTaskInError(taskID)
		if err2 != nil {
			e.logs <- displayMsg(e.name, err2.Error())
		} else {
			e.logs <- displayLine(taskID, e.name, err.Error())
		}
		return
	}
	err = cmd.Wait()
	if err != nil {
		log.Error(err)
		e.markTaskInError(taskID)
		e.logs <- displayLine(taskID, e.name, err.Error())
		return
	}

	e.markTaskAsDone(taskID)
}

func (e *TasksExecutor) markTaskAsDone(taskID string) {
	e.lock.Lock()
	defer e.lock.Unlock()

	t := e.tasks[taskID]
	t.State = "success"
	e.tasks[taskID] = t
}

func (e *TasksExecutor) markTaskInError(taskID string) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	t, is := e.tasks[taskID]
	if !is {
		return fmt.Errorf("Task %s not found", taskID)
	}
	t.State = "error"
	e.tasks[taskID] = t
	return nil
}

func (e *TasksExecutor) ps() (string, error) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	ntasks := len(e.tasks)
	if ntasks == 0 {
		return "0 tasks", nil
	}

	t := make([]string, ntasks)
	i := 0
	for _, task := range e.tasks {
		t[i] = fmt.Sprintf("%s (id=%s state=%s)", task.Command, task.ID, task.State)
		i++
	}

	return strings.Join(t, "<br>"), nil
}

func (e *TasksExecutor) attach(args []string) (string, error) {
	if len(args) != 2 {
		return "missing taskID", nil
	}
	taskID := args[1]

	e.lock.RLock()
	defer e.lock.RUnlock()
	task, is := e.tasks[taskID]
	if !is {
		return fmt.Sprintf("task %s not found", taskID), nil
	}

	if task.State != "running" {
		return fmt.Sprintf("task %s not running", taskID), nil
	}

	e.logs <- displayCmd(taskID, e.name, "ek attach "+task.Command)
	return "", nil
}

func (e *TasksExecutor) kill(args []string) (string, error) {
	if len(args) != 2 {
		return "missing taskID", nil
	}
	taskID := args[1]

	e.lock.RLock()
	defer e.lock.RUnlock()
	task, is := e.tasks[taskID]
	if !is {
		return fmt.Sprintf("task %s killed", taskID), nil
	}

	err := task.stop()
	if err != nil {
		return fmt.Sprintf("fail to stop task %s: %s", taskID, err.Error()), nil
	}
	return fmt.Sprintf("task %s killed", taskID), nil
}

func (e *TasksExecutor) gc() (string, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	ntasks := len(e.tasks)

	for _, task := range e.tasks {
		task.stop()
	}

	e.tasks = map[string]*Task{}

	return fmt.Sprintf("%d tasks removed", ntasks), nil
}

func displayCmd(taskID string, user string, argz string) []byte {
	return []byte(fmt.Sprintf(
		`{"user": "%s", "message":"%s", "b64":"%t"}`,
		user,
		`<pre id='task-`+botID+"-"+taskID+`'>&gt; `+argz+`</pre>`,
		false))
}

func displayLine(taskID string, user string, line string) []byte {
	return []byte(fmt.Sprintf(
		`{"user": "%s", "message":"%s", "b64":"%t", "id":"#task-%s-%s"}`,
		user,
		base64.StdEncoding.EncodeToString([]byte(line)),
		true, botID, taskID))
}

func displayMsg(user string, msg string) []byte {
	return []byte(fmt.Sprintf(
		`{"user": "%s", "message":"%s", "b64":"%t"}`,
		user,
		base64.StdEncoding.EncodeToString([]byte(msg)),
		true))
}
