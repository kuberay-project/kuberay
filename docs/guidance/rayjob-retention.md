# Independent RayCluster cleanup and RayJob retention

Kueue-managed RayJobs that create their own `rayClusterSpec` require
`shutdownAfterJobFinishes: true`. To release cluster resources promptly while
retaining the RayJob CR for debugging, combine standard shutdown with
`DeleteSelf` deletion rules. The `RayJobDeletionPolicy` feature gate must be enabled.

Add the following fields to a RayJob with its own `rayClusterSpec`:

```yaml
spec:
  shutdownAfterJobFinishes: true
  ttlSecondsAfterFinished: 300
  deletionStrategy:
    deletionRules:
      - policy: DeleteSelf
        condition:
          jobDeploymentStatus: Complete
          ttlSeconds: 2592000 # 30 days
      - policy: DeleteSelf
        condition:
          jobDeploymentStatus: Failed
          ttlSeconds: 2592000 # 30 days
```

The RayCluster is deleted 300 seconds after completion. The RayJob CR remains
until 30 days after completion. For a test job retained for 7 days, set both
rule TTLs to `604800`. All deadlines are measured from `status.endTime`.

`jobDeploymentStatus: Failed` also covers failures before job submission, when
`jobStatus` might not be `FAILED`. Cluster shutdown still occurs if no retention
rule matches; in that case, the RayJob CR remains until separately deleted.

With `shutdownAfterJobFinishes: true`, only `DeleteSelf` rules are supported, and
each rule's TTL must be at least `ttlSecondsAfterFinished`. Other rules, such as
`DeleteWorkers` and `DeleteCluster`, require `shutdownAfterJobFinishes: false`.
Legacy `onSuccess`/`onFailure` policies cannot be combined with deletion rules.
Explicit deletion rules take precedence over the legacy
`DELETE_RAYJOB_CR_AFTER_JOB_FINISHES` environment variable.

Suspension still removes the cluster immediately, without deleting the RayJob or
starting its retention clock. After resumption, cleanup deadlines apply when the
job reaches a terminal deployment state. If the controller resumes after both
deadlines have passed, it can delete the RayJob directly; Kubernetes garbage
collection then removes its owned resources.
