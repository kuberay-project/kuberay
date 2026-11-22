package e2eincrementalupgrade

const defaultWorkerCPURequest = "2"

// These parameters control capacity scaling and gradual traffic migration during the upgrade.
type incrementalUpgradeParams struct {
	Name     string
	StepSize int32
	Interval int32
	MaxSurge int32
}

// incrementalUpgradeCombinations defines diverse (stepSize, interval, maxSurge) combinations
// to exercise different upgrade behaviors. Each combination targets a distinct scenario.
var incrementalUpgradeCombinations = []incrementalUpgradeParams{
	{
		// Scenario: Instant cutover.
		// All capacity and traffic shift in one step, which behaves like a blue/green deployment.
		StepSize: 100,
		Interval: 1,
		MaxSurge: 100,
		Name:     "BlueGreen",
	},
	{
		// Scenario: Aggressive gradual upgrade.
		// Larger traffic migration steps with shorter intervals.
		StepSize: 25,
		Interval: 5,
		MaxSurge: 50,
		Name:     "AggressiveGradual",
	},
	{
		// Scenario: Conservative gradual upgrade.
		// Smaller traffic migration steps with longer intervals.
		StepSize: 5,
		Interval: 10,
		MaxSurge: 25,
		Name:     "ConservativeGradual",
	},
}

// ptrs returns (*stepSize, *interval, *maxSurge) for use with the RayService bootstrap helper.
func (p incrementalUpgradeParams) ptrs() (*int32, *int32, *int32) {
	return new(p.StepSize), new(p.Interval), new(p.MaxSurge)
}

// The following defines the Serve configurations for different types of incremental upgrade tests, including:
//   - Functional test
//   - High-RPS Locust load test
//
// NOTE: working_dir is coupled with the external GitHub repos, which might lead to CI flakiness considering the
// availability and stability of these repos and specific commit hashes.

type serveConfigV2 string

// defaultIncrementalUpgradeServeConfigV2 configures a Serve app for functional tests.
const defaultIncrementalUpgradeServeConfigV2 serveConfigV2 = `applications:
  - name: fruit_app
    import_path: fruit.deployment_graph
    route_prefix: /fruit
    runtime_env:
      working_dir: "https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip"
    deployments:
      - name: MangoStand
        num_replicas: 1
        user_config:
          price: 3
        ray_actor_options:
          num_cpus: 0.1
      - name: OrangeStand
        num_replicas: 1
        user_config:
          price: 2
        ray_actor_options:
          num_cpus: 0.1
      - name: FruitMarket
        num_replicas: 1
        ray_actor_options:
          num_cpus: 0.1
  - name: math_app
    import_path: conditional_dag.serve_dag
    route_prefix: /calc
    runtime_env:
      working_dir: "https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip"
    deployments:
      - name: Adder
        num_replicas: 1
        user_config:
          increment: 3
        ray_actor_options:
          num_cpus: 0.1
      - name: Multiplier
        num_replicas: 1
        user_config:
          factor: 5
        ray_actor_options:
          num_cpus: 0.1
      - name: Router
        num_replicas: 1
        ray_actor_options:
          num_cpus: 0.1
`

// highRPSServeConfigV2 configures a minimal high-RPS Serve app (SimpleDeployment) for Locust load tests.
const highRPSServeConfigV2 serveConfigV2 = `applications:
  - name: simple_app
    import_path: locust_test.simple_serve:app
    route_prefix: /test
    runtime_env:
      working_dir: "https://github.com/ray-project/serve_config_examples/archive/530e247ca195530b71b92d7e708048a1bdc02583.zip"
    deployments:
      - name: SimpleDeployment
        max_ongoing_requests: 6
        autoscaling_config:
          min_replicas: 1
          max_replicas: 2
          target_ongoing_requests: 2
          upscale_delay_s: 0.5
        ray_actor_options:
          num_cpus: 2
`
