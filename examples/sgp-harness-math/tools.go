package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go/token"
	"go/types"
	"strings"
)

func toolDefinitions() []ollamaTool {
	return []ollamaTool{
		{
			Type: "function",
			Function: ollamaToolDef{
				Name:        "calculate",
				Description: "Evaluate a simple mathematical expression and return the result. Supports +, -, *, /, and parentheses.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"expression": map[string]any{
							"type":        "string",
							"description": "Mathematical expression to evaluate, e.g. \"2 + 3 * 4\"",
						},
					},
					"required": []string{"expression"},
				},
			},
		},
		{
			Type: "function",
			Function: ollamaToolDef{
				Name:        "teleport",
				Description: "Transfer this session to another harness binary. Pass the absolute path to the destination binary as destination. Call this tool alone, not combined with other tools.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"destination": map[string]any{
							"type":        "string",
							"description": "Absolute path to the destination harness binary",
						},
					},
					"required": []string{"destination"},
				},
			},
		},
	}
}

func executeTool(_ context.Context, name, argsJSON string) (string, bool) {
	var args map[string]any
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return fmt.Sprintf("invalid arguments: %v", err), false
	}

	switch name {
	case "calculate":
		return execCalculate(args)
	default:
		return "unknown tool: " + name, false
	}
}

func execCalculate(args map[string]any) (string, bool) {
	expr, ok := args["expression"].(string)
	if !ok || strings.TrimSpace(expr) == "" {
		return "calculate: expression argument is required", false
	}

	// Use go/types constant evaluation for safe expression parsing.
	tv, err := types.Eval(token.NewFileSet(), nil, token.NoPos, expr)
	if err != nil {
		return fmt.Sprintf("calculate: %v", err), false
	}
	if tv.Value == nil {
		return "calculate: could not evaluate expression", false
	}
	return tv.Value.String(), true
}
