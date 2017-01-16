package main

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/thbkrkr/qli/bot"
)

var (
	name      = "ek"
	buildDate = "dev"
	gitCommit = "dev"

	topic string

	executor TasksExecutor

	botID string
)

func init() {
	flag.StringVar(&topic, "t", "", "Topic (override $T)")
	flag.Parse()
}

func main() {
	hostname, _ := os.Hostname()

	if topic != "" {
		os.Setenv("T", topic)
	}

	botID = hostname
	b := bot.NewBot(fmt.Sprintf("ek~bot@%s-bot2", hostname))

	executor = TasksExecutor{
		name:  b.Name,
		pub:   b.Pub,
		tasks: map[string]Task{},
		lock:  sync.RWMutex{},
	}

	b.RegisterCmdFunc("ek ps", func(args ...string) string {
		return executor.ps()
	}).RegisterCmdFunc("ek attach", func(args ...string) string {
		return executor.attach(args)
	}).RegisterCmdFunc("ek gc", func(args ...string) string {
		return executor.gc()
	}).RegisterCmdFunc("ek dps", func(args ...string) string {
		cmd := []string{"docker", "ps", "-a", "--format", `'table{{.Names}}\t{{.Status}}'`}
		return executor.exec(cmd)
	}).RegisterCmdFunc("ek", func(args ...string) string {
		return executor.exec(args)
	}).Start()
}

//

type TasksExecutor struct {
	name  string
	pub   chan<- string
	tasks map[string]Task
	lock  sync.RWMutex
	total int
}

type Task struct {
	ID      string
	Command string
	State   string
}

func (e *TasksExecutor) exec(args []string) string {
	if len(args) == 0 {
		return "ping!"
	}
	go e.execCommand(args)
	return ""
}

func (e *TasksExecutor) execCommand(args []string) {
	taskID := fmt.Sprintf("%d", e.total)
	e.total++

	cmdArgs := ""
	for _, arg := range args {
		cmdArgs = cmdArgs + arg + " "
	}

	e.startTask(taskID, cmdArgs)

	// Display the command to execute in a pre with the command id
	e.pub <- displayCmd(taskID, e.name, cmdArgs)

	log.WithField("cmd", args).Info("exec cmd")
	cmd := exec.Command(args[0], args[1:]...)
	stdoutReader, err := cmd.StdoutPipe()
	if err != nil {
		log.WithField("cmd", args[0]).WithError(err).Error("Error creating stdout pipe")
		e.pub <- displayLine(taskID, e.name, err.Error())
		return
	}
	stderrReader, err := cmd.StderrPipe()
	if err != nil {
		log.WithField("cmd", args[0]).WithError(err).Error("Error creating stderr pipe")
		e.pub <- displayLine(taskID, e.name, err.Error())
		return
	}

	// Stream command execution
	scanner := bufio.NewScanner(stdoutReader)
	go func() {
		for scanner.Scan() {
			e.pub <- displayLine(taskID, e.name, scanner.Text())
		}
	}()
	scannerStderr := bufio.NewScanner(stderrReader)
	go func() {
		for scannerStderr.Scan() {
			e.pub <- displayLine(taskID, e.name, scannerStderr.Text())
		}
	}()

	err = cmd.Start()
	if err != nil {
		log.Error(err)
		e.markTaskInError(taskID)
		e.pub <- displayLine(taskID, e.name, err.Error())
		return
	}
	err = cmd.Wait()
	if err != nil {
		log.Error(err)
		e.markTaskInError(taskID)
		e.pub <- displayLine(taskID, e.name, err.Error())
		return
	}

	e.markTaskAsDone(taskID)
}

func (e *TasksExecutor) gc() string {
	e.lock.Lock()
	defer e.lock.Unlock()

	ntasks := len(e.tasks)
	e.tasks = map[string]Task{}

	return fmt.Sprintf("%d tasks removed", ntasks)
}

func (e *TasksExecutor) startTask(taskID string, cmd string) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.tasks[taskID] = Task{
		ID:      taskID,
		Command: cmd,
		State:   "running",
	}
}

func (e *TasksExecutor) markTaskAsDone(taskID string) {
	e.lock.Lock()
	defer e.lock.Unlock()
	t := e.tasks[taskID]
	t.State = "success"
	e.tasks[taskID] = t
}

func (e *TasksExecutor) markTaskInError(taskID string) {
	e.lock.Lock()
	defer e.lock.Unlock()
	t := e.tasks[taskID]
	t.State = "error"
	e.tasks[taskID] = t
}

func displayCmd(taskID string, user string, argz string) string {
	return fmt.Sprintf(
		`{"user": "%s", "message":"%s", "b64":"%t"}`,
		user,
		`<pre id='task-`+botID+"-"+taskID+`'>&gt; `+argz+`</pre>`,
		false)
}

func displayLine(taskID string, user string, line string) string {
	return fmt.Sprintf(
		`{"user": "%s", "message":"%s", "b64":"%t", "id":"#task-%s-%s"}`,
		user,
		base64.StdEncoding.EncodeToString([]byte(line)),
		true, botID, taskID)
}

func (e *TasksExecutor) ps() string {
	e.lock.RLock()
	defer e.lock.RUnlock()

	t := make([]string, len(e.tasks))
	i := 0
	for _, task := range e.tasks {
		t[i] = fmt.Sprintf("%s (id=%s state=%s)", task.Command, task.ID, task.State)
		i++
	}

	return strings.Join(t, ", ")
}

func (e *TasksExecutor) attach(args []string) string {
	if len(args) != 2 {
		return "missing taskID"
	}
	taskID := args[1]
	task, is := e.tasks[taskID]
	if !is {
		return "task not found"
	}
	e.pub <- displayCmd(taskID, e.name, "ek attach "+task.Command)
	return ""
}
