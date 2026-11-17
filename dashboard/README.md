# KubeRay Dashboard

This is the repo for the open source dashboard for KubeRay

![image](https://github.com/user-attachments/assets/3c71169d-44c6-45ee-907d-b8a44043b861)

## How to deploy with KubeRay Operator and API Server

First, clone the KubeRay repo and cd to the `apiserver` folder. Then, create a local cluster and install the KubeRay
components by running

```bash
make cluster operator-image load-operator-image deploy-operator install
kubectl -n ray-system rollout status deploy/kuberay-apiserver
```

The API server is then available at `http://localhost:31888/apis/ray.io/v1/namespaces/default/rayjobs`.

Now, to deploy the dashboard, `cd` to the `dashboard` folder and run:

```bash
yarn
yarn dev
```

Open [http://localhost:3000/jobs](http://localhost:3000/jobs). The tables are empty until you create resources
(run `kubectl` from the repository root):

- **Jobs**: Use Create Job in the UI, or
  `kubectl apply -f ray-operator/config/samples/ray-job.sample.yaml`
- **Clusters**: There is no create-cluster button; apply
  `kubectl apply -f ray-operator/config/samples/ray-cluster.sample.yaml`
  and open `/clusters`. A RayJob also creates a cluster, so the job sample appears there too.
- **History**: This UI still has a `/history` tab, but the supported path is the
  [Ray Dashboard + History Server](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/kuberay-history-server.html).

## What works

You can view the list of Ray jobs and Ray clusters. You can search and filter them using frontend
components. You can delete them using the select button. You can also create a test job, but the
compute templates don't work yet. Links to the Ray head dashboard are available once the cluster
service is ready.

## What doesn't work

The Grafana link is empty because the dashboard does not yet let users customize metrics URL
templates for their observability setup.

We don't have a detailed view of each job/cluster. We also need to add a namespace selector since
it's using the "default" namespace right now.

## Note on open source

There are currently roblox-only components that are hidden with a flag in the codebase. We want to remove them
eventually while keeping the roblox fork easily synced with the OSS branch. Any suggestions for this are welcome.

## Tech stack

### Language ➜ Typescript

Should be more maintainable than JS!

### Library ➜ React

We choose React as our javascript library since it’s the industry standard. It’s great for creating interactive Single
Page Applications (SPA).

### Framework ➜ Next.js

We need a framework to handle the tooling and configuration needed for React. Next.js has replaced create-react-app as
the standard React framework.

Since we don’t need SEO, we can build it as a SPA instead of using SSR (server-side rendering), which Next.js is known
for. Nonetheless, Next.js is still good for building SPAs, since it provides a file-system based router, fast local
development with TurboPack, a performant Rust-based transpiler called SWC, and built-in optimizations for asset-loading
(such as fonts and images). The app uses the App Router with client components (`"use client"`) rather than SSR.

### UI Framework ➜ MUI (Joy UI)

Between Material UI, which has an older Google-designed look that is also used by Kubeflow, and NextUI, which is more
modern, we choose Joy UI, which sits somewhere in the middle. Joy UI is a modified version of MUI with a clean and
modern look that is not too out-of-place with the Kubeflow interface.

### Data Fetching ➜ SWR

While we can directly use JavaScript fetch() to fetch data, Vercel’s SWR hook is easier to use and more feature-rich.
SWR makes it easy to continuously fetch the latest data to keep the frontend up-to-date without manual refreshing, and
it also uses caching with the stale-while-revalidate strategy to provide a smooth user experience. In addition, SWR
simplifies our data-fetching code to be more maintainable.

Since our app is a dashboard, we want to revalidate the data every 5s like Grafana. Since we care more about accurate
information than UI latency, we won’t use the optimistic UI feature for POST requests, which mutates data immediately
and only rollback if the request didn’t go through. Instead, we will show a spinner for every API call like Kubeflow
does.
