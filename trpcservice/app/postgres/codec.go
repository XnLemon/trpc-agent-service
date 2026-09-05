package postgres

import appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"

func encodeAgentRevisionParts(revision appmodel.Revision) ([]byte, []byte, []byte, error) {
	generation, err := encodeJSON(revision.Generation)
	if err != nil {
		return nil, nil, nil, err
	}
	runtime, err := encodeJSON(revision.Runtime)
	if err != nil {
		return nil, nil, nil, err
	}
	tools, err := encodeJSON(revision.Tools)
	if err != nil {
		return nil, nil, nil, err
	}
	return generation, runtime, tools, nil
}

func decodeAgentRevisionParts(generation, runtime []byte, revision *appmodel.Revision) error {
	if err := decodeJSON(generation, &revision.Generation); err != nil {
		return err
	}
	return decodeJSON(runtime, &revision.Runtime)
}
