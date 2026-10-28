#!/bin/bash

# Suppress interactive prompts
export DEBIAN_FRONTEND=noninteractive
export TZ=America/Los_Angeles

curl -o- https://get.docker.com | sh
