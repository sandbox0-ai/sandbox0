.PHONY: help docker-build docker-push docker-build-% docker-push-%

# Image repository (default). Override if needed:
#   make docker-build-default IMAGE_REPO=your/repo TAG=v0.0.1
IMAGE_REPO ?= sandbox0ai/otemplates

# Base tag. The final pushed tag is "<template>-<TAG>".
#   make docker-build-default TAG=v0.0.1  -> sandbox0ai/otemplates:default-v0.0.1
# For compatibility with other infra Makefiles, VERSION can also be used.
TAG ?= $(VERSION)
TAG ?= latest

TEMPLATES := $(patsubst Dockerfile-%,%,$(notdir $(wildcard Dockerfile-*)))

define IMAGE_TAG
$(1)-$(TAG)
endef

# Convenience aliases: default template (if present).
docker-build:
	@$(MAKE) docker-build-default

docker-push:
	@$(MAKE) docker-push-default

docker-build-%:
	@if [ ! -f "Dockerfile-$*" ]; then \
		echo "Error: Dockerfile-$* not found."; \
		echo "Available templates: $(TEMPLATES)"; \
		exit 2; \
	fi
	docker build -t $(IMAGE_REPO):$(call IMAGE_TAG,$*) -f Dockerfile-$* .

docker-push-%:
	docker push $(IMAGE_REPO):$(call IMAGE_TAG,$*)

help:
	@echo "Usage:"
	@echo "  make docker-build-<template> TAG=<tag>   Build image $(IMAGE_REPO):<template>-<tag>"
	@echo "  make docker-push-<template>  TAG=<tag>   Push  image $(IMAGE_REPO):<template>-<tag>"
	@echo ""
	@echo "Examples:"
	@echo "  make docker-build-default TAG=v0.0.1"
	@echo "  make docker-push-default  TAG=v0.0.1"
	@echo ""
	@echo "Templates (from Dockerfile-*): $(TEMPLATES)"
	@echo ""
	@echo "Variables:"
	@echo "  TAG        Base tag (default: latest). Also supports VERSION as an alias."
	@echo "  IMAGE_REPO Image repository (default: $(IMAGE_REPO))"
