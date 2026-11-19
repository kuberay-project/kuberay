## Highlights

### {{Feature}} ({{alpha|beta}})

{{What it does, why it matters, what changed since last release if promoted. Fold alpha-stage churn of this feature into this paragraph; do not list it elsewhere.}}

Try it here: [{{Doc title}}]({{docs.ray.io URL}}).

> :warning: **Warning:** {{FeatureGate}} is an **Alpha** feature and is **disabled by default**. To enable it, set the feature gate on the kuberay-operator:
>
> ```
> --feature-gates={{FeatureGate}}=true
> ```

```yaml
{{sample excerpt}}
```

See [{{sample_file}}](https://github.com/kuberay-project/kuberay/blob/master/ray-operator/config/samples/{{sample_file}}) for a full example.

### Other Notable Features

<!-- One `####` group per component that has entries; headings and order are listed in SKILL.md. -->

#### {{Component}}

* Added `{{field}}` to `{{parent}}` for {{purpose}}. See [{{sample}}]({{url}}). (#{{PR}})

### Notable Bug Fixes

* **{{Component}}**: Fixed a bug where {{symptom}}. (#{{PR}})

### Breaking Changes

* **{{Area}}**: {{what changed}}. {{What users must do}} (#{{PR}})

### Deprecations

* {{What is deprecated and what to migrate to}} (#{{PR}})

## Changelog

<!-- Click "Generate release notes" in the GitHub release UI and paste the "What's Changed" list here -->
