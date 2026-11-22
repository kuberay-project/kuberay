import {
  defaultConfig,
  apiVersion,
  type RuntimeConfig,
} from "./config-defaults";

export function getHistoryServerConfig(): {
  domain: string;
  apiPath: string;
  proxyEndpoint: string;
} {
  return {
    domain:
      process.env.NEXT_PUBLIC_HISTORYSERVER_DOMAIN ||
      process.env.HISTORYSERVER_DOMAIN ||
      defaultConfig.historyserver.domain,
    apiPath:
      process.env.NEXT_PUBLIC_HISTORYSERVER_API_PATH ||
      process.env.HISTORYSERVER_API_PATH ||
      defaultConfig.historyserver.apiPath,
    proxyEndpoint:
      process.env.NEXT_PUBLIC_HISTORYSERVER_PROXY_ENDPOINT ||
      process.env.HISTORYSERVER_PROXY_ENDPOINT ||
      defaultConfig.historyserver.proxyEndpoint,
  };
}

export function getServerConfig(): RuntimeConfig {
  // Support legacy API_URL env var for backward compatibility
  const legacyApiUrl = process.env.NEXT_PUBLIC_API_URL || process.env.API_URL;

  const historyserver = getHistoryServerConfig();

  const defaultNamespace =
    process.env.NEXT_PUBLIC_DEFAULT_NAMESPACE ||
    process.env.DEFAULT_NAMESPACE ||
    defaultConfig.defaultNamespace;

  if (legacyApiUrl) {
    // Parse legacy URL format
    try {
      const url = new URL(legacyApiUrl);
      return {
        apiserver: {
          domain: `${url.protocol}//${url.host}`,
          rayApiPath: url.pathname,
          coreApiPath: apiVersion === "v2" ? "/api/v1" : undefined,
        },
        historyserver,
        defaultNamespace,
      };
    } catch {
      // If not a valid URL, return as domain
      return {
        apiserver: {
          domain: legacyApiUrl,
          rayApiPath: defaultConfig.apiserver.rayApiPath,
          coreApiPath: defaultConfig.apiserver.coreApiPath,
        },
        historyserver,
        defaultNamespace,
      };
    }
  }

  // New format with separated domain and path
  return {
    apiserver: {
      domain:
        process.env.NEXT_PUBLIC_API_DOMAIN ||
        process.env.API_DOMAIN ||
        defaultConfig.apiserver.domain,
      rayApiPath:
        process.env.NEXT_PUBLIC_RAY_API_PATH ||
        process.env.RAY_API_PATH ||
        defaultConfig.apiserver.rayApiPath,
      coreApiPath:
        process.env.NEXT_PUBLIC_CORE_API_PATH ||
        process.env.CORE_API_PATH ||
        defaultConfig.apiserver.coreApiPath,
    },
    historyserver,
    defaultNamespace,
  };
}
