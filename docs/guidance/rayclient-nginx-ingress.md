<!-- markdownlint-disable MD013 -->
# Connect to Ray client with NGINX Ingress
>
> Warning: Ray client has some known [limitations](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html#things-to-know) and is not actively maintained.

This document provides an example for connecting Ray client to a Raycluster via [NGINX Ingress](https://kubernetes.github.io/ingress-nginx/) on Kind. Although this is a Kind example, the steps apply to any Kubernetes Cluster that runs the NGINX Ingress Controller.

## Requirements

* Environment:
  * `Ubuntu`
  * `Kind`

* Computing resources:
  * 16GB RAM
  * 8 CPUs

## Step 1: Create a Kind cluster

The extra arg prepares the Kind cluster for deploying the ingress controller

```sh
cat <<EOF | kind create cluster --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  - containerPort: 80
    hostPort: 80
    protocol: TCP
  - containerPort: 443
    hostPort: 443
    protocol: TCP
EOF
```

## Step 2: Deploy NGINX Ingress Controller

The [SSL Passthrough feature](https://kubernetes.github.io/ingress-nginx/user-guide/tls/#ssl-passthrough) is required to pass on the encryption to the backend service directly.

```sh
# Deploy the NGINX Ingress Controller
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

# Turn on SSL Passthrough
kubectl patch deploy --type json --patch '[{"op":"add","path": "/spec/template/spec/containers/0/args/-","value":"--enable-ssl-passthrough"}]' ingress-nginx-controller -n ingress-nginx

# Verify log has Starting TLS proxy for SSL Passthrough
kubectl logs deploy/ingress-nginx-controller -n ingress-nginx
```

## Step 3: Install KubeRay operator

Follow this [document](../../helm-chart/kuberay-operator/README.md) to install the latest stable KubeRay operator via Helm repository.

## Step 4: Create a Raycluster with TLS enabled

The Ray client server is a GRPC service. The NGINX Ingress Controller supports GRPC backend service which uses http/2 and requires secured connection. The command below creates a Raycluster with TLS enabled:

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.tls.yaml
```

Refer to the [TLS document](tls.md) for more detail.

## Step 5: Create an ingress for the Ray client service

With the Raycluster running, create an ingress for the Ray client backend service using the [rayclient-ingress](../../ray-operator/config/samples/ingress-rayclient-tls.yaml) example below:

```sh
cat << EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: rayclient-ingress
  annotations:
    kubernetes.io/ingress.class: nginx
    nginx.ingress.kubernetes.io/ssl-passthrough: "true"
    nginx.ingress.kubernetes.io/backend-protocol: "GRPC"
spec:
  rules:
    - host: "localhost"
      http:
        paths:
        - path: /
          pathType: Prefix
          backend:
            service:
              name: raycluster-tls-head-svc
              port:
                number: 10001
EOF
```

The annotation, `nginx.ingress.kubernetes.io/backend-protocol: "GRPC"` sets up the appropriate NGINX configuration to route http/2 traffic to a GRPC backend service. The `nginx.ingress.kubernetes.io/ssl-passthrough: "true"` annotation tells the ingress to forward the encrypted traffic to the backend service to be handled inside the Raycluster itself.

## Step 6: Connecting to Ray client service via the ingress

Since the Raycluster uses TLS, the local Ray client would require a set of certificates to connect to Raycluster.
> Warning: Ray client has some known [limitations](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html#things-to-know) and is not actively maintained.

```sh
# Download the ca key pair and create a cert signing request (CSR)
kubectl get secret ca-tls -o template='{{index .data "ca.key"}}'|base64 -d > ./ca.key
kubectl get secret ca-tls -o template='{{index .data "ca.crt"}}'|base64 -d > ./ca.crt
openssl req -nodes -newkey rsa:2048 -keyout ./tls.key -out ./tls.csr -subj '/CN=local'
cat <<EOF >./cert.conf
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF
# Sign and create a tls cert
openssl x509 -req -CA ./ca.crt -CAkey ./ca.key -in ./tls.csr -out ./tls.crt -days 365 -CAcreateserial -extfile ./cert.conf

# Connect Ray client to the Raycluster using the tls keypair and the ca cert
python -c '
import os
import ray
os.environ["RAY_USE_TLS"] = "1"
os.environ["RAY_TLS_SERVER_CERT"] = os.path.join("./", "tls.crt")
os.environ["RAY_TLS_SERVER_KEY"] = os.path.join("./", "tls.key")
os.environ["RAY_TLS_CA_CERT"] = os.path.join("./", "ca.crt")
ray.init(address="ray://localhost", logging_level="DEBUG")'
```

The output should be similar to:

```text
2023-04-25 16:33:32,452   INFO client_builder.py:253 -- Passing the following kwargs to ray.init() on the server: logging_level
2023-04-25 16:33:32,460   DEBUG worker.py:378 -- client gRPC channel state change: ChannelConnectivity.IDLE
2023-04-25 16:33:32,664   DEBUG worker.py:378 -- client gRPC channel state change: ChannelConnectivity.CONNECTING
2023-04-25 16:33:32,671   DEBUG worker.py:378 -- client gRPC channel state change: ChannelConnectivity.READY
```
