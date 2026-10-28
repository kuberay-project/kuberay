# Grafana dashboards

The grafana dashboards in this directory are copied over from the ray repo.

## Updating the dashboards

To update the dashboards with the latest dashboards from ray, run the following command:

Install ray:

```bash
pip install "ray[default]"
```

Run locally:

```bash
ray start --head
```

Copy the dashboards to this directory:

```bash
cp -r /tmp/ray/session_latest/metrics/grafana/dashboards/* .
```
