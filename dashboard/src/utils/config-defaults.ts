export interface RuntimeConfig {
  apiserver: {
    domain: string;
    rayApiPath: string;
    coreApiPath?: string;
  };
  historyserver: {
    domain: string;
    apiPath: string;
    proxyEndpoint: string;
  };
  defaultNamespace?: string;
}

// apiserver config defaults
export const apiVersion = "v2";
export const defaultDomain = "http://localhost:31888";
export const v2RayApiPath = "/apis/ray.io/v1";
export const v1RayApiPath = "/apis/v1";
export const v2CoreApiPath = "/api/v1";

// history server config defaults
export const historyServerApiPath = "/api/v0";
export const historyServerDomain = "http://localhost:8080";
export const proxyEndpoint = "/api/historyserver";
export const defaultConfig: RuntimeConfig = {
  apiserver: {
    domain: defaultDomain,
    rayApiPath: apiVersion === "v2" ? v2RayApiPath : v1RayApiPath,
    coreApiPath: apiVersion === "v2" ? v2CoreApiPath : undefined,
  },
  historyserver: {
    domain: historyServerDomain,
    apiPath: historyServerApiPath,
    proxyEndpoint,
  },
};

export const ALL_NAMESPACES = "all";
