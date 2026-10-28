import os
import sys
from yaml import load, CSafeLoader
from deepdiff import DeepDiff

def compare_two_yaml(yaml1, yaml2):
	diff = DeepDiff(yaml1['rules'], yaml2['rules'])
	if diff:
		print(diff)
	return not diff

if __name__ == "__main__":
	curr_dir_path = os.path.dirname(os.path.realpath(__file__))
	helm_rbac_dir = curr_dir_path + '/tmp/'
	kustomize_rbac_dir = curr_dir_path + '/../ray-operator/config/rbac/'

	os.system(f"{curr_dir_path}/helm-render-yaml.sh")
	files = os.listdir(helm_rbac_dir)
	diff_files = []

	for f in files:
		yaml1 = load(open(helm_rbac_dir + f, 'r'), Loader=CSafeLoader)
		yaml2 = load(open(kustomize_rbac_dir + f, 'r'), Loader=CSafeLoader)
		if not compare_two_yaml(yaml1, yaml2):
			diff_files.append(f)

	if diff_files:
		sys.exit(f"{diff_files} are out of synchronization! RBAC YAML files in" +
			"\'helm-chart/kuberay-operator/templates\' and \'ray-operator/config/rbac\'" +
			"should be synchronized manually. See DEVELOPMENT.md for more details.")
