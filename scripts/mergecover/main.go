// Command mergecover combines Go coverage profiles by retaining the highest
// execution count for each source block. It is used when an integration-test
// package exercises implementations that deliberately live in other modules.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fail(err)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("mergecover", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	output := flags.String("out", "", "output coverage profile")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *output == "" || flags.NArg() == 0 {
		return errors.New("usage: mergecover -out merged.out profile.out [profile.out ...]")
	}

	mode, blocks, order, err := mergeProfiles(flags.Args())
	if err != nil {
		return err
	}
	return writeProfile(*output, mode, blocks, order)
}

func writeProfile(output, mode string, blocks map[string]int, order []string) error {
	file, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("create %s: %w", output, err)
	}
	writer := bufio.NewWriter(file)
	if _, err := fmt.Fprintf(writer, "mode: %s\n", mode); err != nil {
		_ = file.Close()
		return err
	}
	for _, block := range mergedCoverageBlockOrder(order, blocks) {
		if _, err := fmt.Fprintf(writer, "%s %d\n", block, blocks[block]); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return nil
}

func mergeProfiles(paths []string) (string, map[string]int, []string, error) {
	mode := ""
	blocks := make(map[string]int)
	order := make([]string, 0)
	seen := make(map[string]struct{})
	for _, path := range paths {
		profileMode, profileBlocks, profileOrder, err := readProfile(path)
		if err != nil {
			return "", nil, nil, err
		}
		if mode == "" {
			mode = profileMode
		} else if mode != profileMode {
			return "", nil, nil, fmt.Errorf("coverage mode mismatch: %q and %q", mode, profileMode)
		}
		for _, block := range profileOrder {
			if _, exists := seen[block]; !exists {
				order = append(order, block)
				seen[block] = struct{}{}
			}
		}
		for block, count := range profileBlocks {
			previous, exists := blocks[block]
			if !exists || count > previous {
				blocks[block] = count
			}
		}
	}
	return mode, blocks, order, nil
}

func readProfile(path string) (string, map[string]int, []string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", nil, nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return "", nil, nil, fmt.Errorf("read %s: missing coverage mode", path)
	}
	modeLine := scanner.Text()
	if !strings.HasPrefix(modeLine, "mode: ") {
		return "", nil, nil, fmt.Errorf("read %s: invalid coverage mode %q", path, modeLine)
	}
	blocks := make(map[string]int)
	order := make([]string, 0)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 {
			return "", nil, nil, fmt.Errorf("read %s: invalid coverage block %q", path, scanner.Text())
		}
		var count int
		if _, err := fmt.Sscanf(fields[2], "%d", &count); err != nil {
			return "", nil, nil, fmt.Errorf("read %s: invalid count: %w", path, err)
		}
		block := fields[0] + " " + fields[1]
		if _, exists := blocks[block]; !exists {
			order = append(order, block)
		}
		blocks[block] = count
	}
	if err := scanner.Err(); err != nil {
		return "", nil, nil, fmt.Errorf("read %s: %w", path, err)
	}
	return strings.TrimPrefix(modeLine, "mode: "), blocks, order, nil
}

func mergedCoverageBlockOrder(original []string, blocks map[string]int) []string {
	order := make([]string, 0, len(blocks))
	seen := make(map[string]struct{}, len(blocks))
	for _, block := range original {
		if _, exists := blocks[block]; exists {
			order = append(order, block)
			seen[block] = struct{}{}
		}
	}
	remaining := make([]string, 0)
	for block := range blocks {
		if _, exists := seen[block]; !exists {
			remaining = append(remaining, block)
		}
	}
	sort.Slice(remaining, func(i, j int) bool { return coverageBlockLess(remaining[i], remaining[j]) })
	return append(order, remaining...)
}

// coverageBlockLess preserves Go's coverage profile ordering. Lexical sorting
// is incorrect for source positions because, for example, line 101 sorts
// before line 16; external coverage consumers require numeric source order.
func coverageBlockLess(left, right string) bool {
	leftLocation, leftOK := parseCoverageBlockLocation(left)
	rightLocation, rightOK := parseCoverageBlockLocation(right)
	if !leftOK || !rightOK {
		return left < right
	}
	if leftLocation.file != rightLocation.file {
		return leftLocation.file < rightLocation.file
	}
	if leftLocation.startLine != rightLocation.startLine {
		return leftLocation.startLine < rightLocation.startLine
	}
	if leftLocation.startColumn != rightLocation.startColumn {
		return leftLocation.startColumn < rightLocation.startColumn
	}
	if leftLocation.endLine != rightLocation.endLine {
		return leftLocation.endLine < rightLocation.endLine
	}
	return leftLocation.endColumn < rightLocation.endColumn
}

type coverageBlockLocation struct {
	file                   string
	startLine, startColumn int
	endLine, endColumn     int
}

func parseCoverageBlockLocation(block string) (coverageBlockLocation, bool) {
	fields := strings.Fields(block)
	if len(fields) == 0 {
		return coverageBlockLocation{}, false
	}
	location := fields[0]
	separator := strings.LastIndex(location, ":")
	if separator < 1 {
		return coverageBlockLocation{}, false
	}
	rangeParts := strings.Split(strings.TrimSpace(location[separator+1:]), ",")
	if len(rangeParts) != 2 {
		return coverageBlockLocation{}, false
	}
	start, startOK := parseCoveragePosition(rangeParts[0])
	end, endOK := parseCoveragePosition(rangeParts[1])
	if !startOK || !endOK {
		return coverageBlockLocation{}, false
	}
	return coverageBlockLocation{
		file: location[:separator], startLine: start.line, startColumn: start.column,
		endLine: end.line, endColumn: end.column,
	}, true
}

type coveragePosition struct{ line, column int }

func parseCoveragePosition(value string) (coveragePosition, bool) {
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return coveragePosition{}, false
	}
	line, lineErr := strconv.Atoi(parts[0])
	column, columnErr := strconv.Atoi(parts[1])
	if lineErr != nil || columnErr != nil {
		return coveragePosition{}, false
	}
	return coveragePosition{line: line, column: column}, true
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
