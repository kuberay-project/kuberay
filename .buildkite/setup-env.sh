#!/bin/bash

# Install Go
export PATH=$PATH:/usr/local/go/bin

# Pin Docker API version
export DOCKER_API_VERSION=1.43

# Install kind
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.31.0/kind-linux-amd64
chmod +x ./kind
mv ./kind /usr/local/bin/kind

# Install Docker
bash scripts/install-docker.sh

# Delete dangling clusters
kind delete clusters --all

# Install kubectl.
curl -LO https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubectl
curl -LO "https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubectl.sha256"
echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Install Helm
curl -Lo helm.tar.gz https://get.helm.sh/helm-v3.12.2-linux-amd64.tar.gz
tar -zxvf helm.tar.gz
mv linux-amd64/helm /usr/local/bin/helm
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

# Install python 3.11 and pip
apt-get update
apt-get install -y python3.11 python3-pip python3-venv python3-dev build-essential

# Install requirements
pip install --break-system-packages ray[default]==2.55.0

# Bypass Git's ownership check due to unconventional user IDs in Docker containers
git config --global --add safe.directory /workdir
