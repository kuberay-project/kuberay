#!/bin/bash

# This script is used to build and load the light-weight submitter image into the kind cluster.
IMG=kuberay/submitter:nightly make docker-image-rayjob-submitter &&
    kind load docker-image kuberay/submitter:nightly
