#!/usr/bin/env bash
if [ -L "${BASH_SOURCE-$0}" ]; then
  PWD=$(dirname "$(readlink "${BASH_SOURCE-$0}")")
else
  PWD=$(dirname "${BASH_SOURCE-$0}")
fi
CURRENT_PATH=$(cd "${PWD}">/dev/null || exit 1; pwd)
export CURRENT_PATH
export KUBERAY_HOME=${CURRENT_PATH}/..

cd "$KUBERAY_HOME"/helm-chart/kuberay-operator/ || exit 1
declare -a YAML_ARRAY=("role.yaml" "ray_rayjob_editor_role.yaml" "ray_rayjob_viewer_role.yaml" "leader_election_role.yaml" "ray_rayservice_editor_role.yaml" "ray_rayservice_viewer_role.yaml" "ray_raycronjob_editor_role.yaml" "ray_raycronjob_viewer_role.yaml" )
mkdir -p "$KUBERAY_HOME"/scripts/tmp
for name in "${YAML_ARRAY[@]}"; do
  helm template -s templates/"$name" . > "$CURRENT_PATH"/tmp/"$name"
done
