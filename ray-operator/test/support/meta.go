package support

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type labelSelector string

var _ Option[*metav1.ListOptions] = (*labelSelector)(nil)

func (l labelSelector) applyTo(options *metav1.ListOptions) error {
	options.LabelSelector = string(l)
	return nil
}

func LabelSelector(selector string) Option[*metav1.ListOptions] {
	return new(labelSelector(selector))
}
