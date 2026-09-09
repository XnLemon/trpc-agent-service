//go:build windows

package tool

import "os/exec"

func configureMCPProcess(_ *exec.Cmd) {}

func killMCPProcess(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	_ = command.Process.Kill()
}
