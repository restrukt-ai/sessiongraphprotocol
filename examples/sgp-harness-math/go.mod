module github.com/restrukt-ai/sessiongraphprotocol/examples/sgp-harness-math

go 1.26.4

require (
	github.com/restrukt-ai/openagentcontainers v0.0.0-00010101000000-000000000000
	github.com/restrukt-ai/sessiongraphprotocol v0.0.0
	golang.org/x/net v0.56.0
)

require (
	connectrpc.com/connect v1.20.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace (
	github.com/restrukt-ai/openagentcontainers => ../../../openagentcontainers
	github.com/restrukt-ai/sessiongraphprotocol => ../..
)
