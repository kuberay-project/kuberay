package runtime

type RayLogCollector interface {
	Run(stop <-chan struct{}) error
}
