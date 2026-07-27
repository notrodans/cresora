OPENAPI_SPEC := api/openapi.yaml
OGEN_OUTPUT := internal/entrypoint/http/ogen

.PHONY: generate-openapi
generate-openapi:
	go tool ogen --target $(OGEN_OUTPUT) --package ogen --clean $(OPENAPI_SPEC)
