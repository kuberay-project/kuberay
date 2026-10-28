#!/bin/bash
set -e
set -x
REPO_ROOT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )"/../.. &> /dev/null && pwd)
readonly REPO_ROOT_DIR
readonly TARGET_DIR="${REPO_ROOT_DIR}/third_party/swagger-ui"
readonly SWAGGER_UI_VERSION=${1:-"5.4.1"}
readonly SWAGGER_UI_TAR_URL="https://github.com/swagger-api/swagger-ui/archive/refs/tags/v${SWAGGER_UI_VERSION}.tar.gz"

if [ -f "${TARGET_DIR}"/swagger-initializer.js ];then
    cp -v "${TARGET_DIR}"/swagger-initializer.js "${TARGET_DIR}"/swagger-initializer.js.backup
fi
echo "Downloading '${SWAGGER_UI_TAR_URL}' to update ${TARGET_DIR}"
tmp="$(mktemp -d)"
#pushd .
curl --output-dir "${tmp}" --fail --silent --location --remote-header-name --remote-name "${SWAGGER_UI_TAR_URL}"
tar -xzvf "${tmp}"/swagger-ui-"${SWAGGER_UI_VERSION}".tar.gz -C "${tmp}"
#popd
cp -rv "$tmp/swagger-ui-${SWAGGER_UI_VERSION}/dist/"* "${TARGET_DIR}"
rm -rf "$tmp"
