OPENAPI_SPEC := api/openapi.yaml
OGEN_OUTPUT := internal/entrypoint/http/ogen
ARCH_LINT_MODULE := github.com/fe3dback/go-arch-lint
ARCH_LINT_VERSION := v1.16.0
ARCH_LINT_BIN ?= $(CURDIR)/bin/go-arch-lint

.PHONY: generate-openapi arch-lint arch arch-verify
generate-openapi:
	go tool ogen --target $(OGEN_OUTPUT) --package ogen --clean $(OPENAPI_SPEC)

# `make arch` installs the pinned checker outside the application module and runs it.
arch-lint:
	@mkdir -p "$(dir $(ARCH_LINT_BIN))"
	@GOBIN="$(dir $(ARCH_LINT_BIN))" go install "$(ARCH_LINT_MODULE)@$(ARCH_LINT_VERSION)"

arch: arch-lint
	"$(ARCH_LINT_BIN)" check --project-path "$(CURDIR)" --arch-file "$(CURDIR)/.go-arch-lint.yml"

# `make arch-verify` proves the negative case, cleans its fixture, then checks normally.
arch-verify: arch-lint
	ARCH_LINT_BIN="$(ARCH_LINT_BIN)" ./scripts/verify-architecture.sh
