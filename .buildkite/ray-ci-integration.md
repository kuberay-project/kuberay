
# Ray CI Integration

| Version                | Latest Ray Release            |  Ray Nightly                         |
| -----------            | :-------------------          | :--------------                      |
| Latest KubeRay Release | During Ray & KubeRay Releases | Nightly from Ray Release Automation  |
| KubeRay Nightly        | In KubeRay CI                 | Not tested                           |

This table lays out the state of testing between Ray and KubeRay nightlies and releases.
The goal is to have all 4 of these being consistently tested eventually.
All tests run in KubeRay CI pipeline, the difference is just where the pipeline is actually kicked off from.
"KubeRay Nightly" just refers to running on master right now, And "Latest KubeRay Release" refers to running
on the latest release branch. The "Latest Ray Release" will be pulled from DockerHub and same with "Ray Nightly".

In the future, if we have a test needs the ray nightly to run, add a step to .buildkite/test-e2e.yaml that
follows the other steps but sets KUBERAY_TEST_RAY_IMAGE env variable to "rayproject/ray:nightly".
When ray releases a new version, you can change the step to just use the latest ray release.
