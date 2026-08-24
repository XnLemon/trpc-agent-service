// Command mergecover combines Go coverage profiles by retaining the highest
// execution count for each source block. It is used when an integration-test
// package exercises implementations that deliberately live in other modules.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

func main() {
	output := flag.String("out", "", "output coverage profile")
	flag.Parse()
	if *output == "" || flag.NArg() == 0 {
		fail(errors.New("usage: mergecover -out merged.out profile.out [profile.out ...]"))
	}

	mode, blocks, err := mergeProfiles(flag.Args())
	if err != nil {
		fail(err)
	}

	file, err := os.Create(*output)
	if err != nil {
		fail(fmt.Errorf("create %s: %w", *output, err))
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	if _, err := fmt.Fprintf(writer, "mode: %s\n", mode); err != nil {
		fail(err)
	}
	keys := make([]string, 0, len(blocks))
	for block := range blocks {
		keys = append(keys, block)
	}
	sort.Strings(keys)
	for _, block := range keys {
		if _, err := fmt.Fprintf(writer, "%s %d\n", block, blocks[block]); err != nil {
			fail(err)
		}
	}
}

func mergeProfiles(paths []string) (string, map[string]int, error) {
	mode := ""
	blocks := make(map[string]int)
	for _, path := range paths {
		profileMode, profileBlocks, err := readProfile(path)
		if err != nil {
			return "", nil, err
		}
		if mode == "" {
			mode = profileMode
		} else if mode != profileMode {
			return "", nil, fmt.Errorf("coverage mode mismatch: %q and %q", mode, profileMode)
		}
		for block, count := range profileBlocks {
			previous, exists := blocks[block]
			if !exists || count > previous {
				blocks[block] = count
			}
		}
	}
	return mode, blocks, nil
}

func readProfile(path string) (string, map[string]int, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return "", nil, fmt.Errorf("read %s: missing coverage mode", path)
	}
	modeLine := scanner.Text()
	if !strings.HasPrefix(modeLine, "mode: ") {
		return "", nil, fmt.Errorf("read %s: invalid coverage mode %q", path, modeLine)
	}
	blocks := make(map[string]int)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 {
			return "", nil, fmt.Errorf("read %s: invalid coverage block %q", path, scanner.Text())
		}
		var count int
		if _, err := fmt.Sscanf(fields[2], "%d", &count); err != nil {
			return "", nil, fmt.Errorf("read %s: invalid count: %w", path, err)
		}
		blocks[fields[0]+" "+fields[1]] = count
	}
	if err := scanner.Err(); err != nil {
		return "", nil, fmt.Errorf("read %s: %w", path, err)
	}
	return strings.TrimPrefix(modeLine, "mode: "), blocks, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
