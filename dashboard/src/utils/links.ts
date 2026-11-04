import { config } from "./constants";

// Name of the Ray dashboard port on the head Service. The operator always adds
// this port to the head Service, either from the ports declared by the head
// container or, when it declares none, from the default ports.
const DASHBOARD_PORT_NAME = "dashboard";

/**
 * Builds the apiserver Service proxy link to the Ray head dashboard, or returns
 * undefined when the cluster doesn't expose one (yet).
 *
 * The port is addressed by name (`<head-svc>:dashboard`) rather than by number
 * so that the link also works for head containers that declare no ports at all,
 * for which the head Service is built from the default ports and no
 * `containerPort` is available in the spec.
 *
 * `status.endpoints` mirrors the head Service ports, so the presence of the
 * `dashboard` key is what tells us the port exists. Its value can't be used to
 * build the link, since it holds the nodePort or targetPort rather than the
 * Service port.
 */
export const buildRayHeadDashboardLink = (
  namespace: string | undefined,
  serviceName: string | undefined,
  endpoints: Record<string, string> | undefined,
): string | undefined => {
  if (!namespace || !serviceName || !config.coreApiUrl) {
    return undefined;
  }
  if (!endpoints || !(DASHBOARD_PORT_NAME in endpoints)) {
    return undefined;
  }
  return `${config.coreApiUrl}/namespaces/${namespace}/services/${serviceName}:${DASHBOARD_PORT_NAME}/proxy/`;
};
