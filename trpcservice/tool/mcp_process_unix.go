//go:build !windows

package tool

import (
	"os/exec"
	"syscall"
)

func configureMCPProcess(command *exec.Cmd) {
	if command == nil {
		return
	}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killMCPProcess(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	// Kill the process group first so a shell-free MCP executable cannot leave
	// descendants behind if it launched helpers of its own. Fall back to the
	// direct process for unusual runtimes where a group was not installed.
	if command.Process.Pid > 0 {
		if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err == nil {
			return
		}
	}
	_ = command.Process.Kill()
}
