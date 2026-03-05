.PHONY: dev build build-prod test test-watch lint format typecheck

dev:
	pnpm dev

build:
	pnpm build

build-prod:
	pnpm build:prod

test:
	pnpm test

test-watch:
	pnpm test:watch

lint:
	pnpm lint

format:
	pnpm format

typecheck:
	pnpm typecheck
