import { RuntimeConfig, defaultConfig, apiVersion } from "./config-defaults";

export { defaultConfig, apiVersion };
export type { RuntimeConfig };

let runtimeConfig: RuntimeConfig | null = null;

export async function fetchRuntimeConfig(): Promise<RuntimeConfig> {
  if (runtimeConfig) {
    return runtimeConfig;
  }

  try {
    const response = await fetch("/api/config");
    if (response.ok) {
      const data: RuntimeConfig = await response.json();
      runtimeConfig = {
        apiserver: {
          domain: data.apiserver?.domain || defaultConfig.apiserver.domain,
          rayApiPath:
            data.apiserver?.rayApiPath || defaultConfig.apiserver.rayApiPath,
          coreApiPath:
            data.apiserver?.coreApiPath !== undefined
              ? data.apiserver.coreApiPath
              : defaultConfig.apiserver.coreApiPath,
        },
        historyserver: {
          domain:
            data.historyserver?.domain || defaultConfig.historyserver.domain,
          apiPath:
            data.historyserver?.apiPath || defaultConfig.historyserver.apiPath,
          proxyEndpoint:
            data.historyserver?.proxyEndpoint ||
            defaultConfig.historyserver.proxyEndpoint,
        },
        defaultNamespace:
          data.defaultNamespace || defaultConfig.defaultNamespace,
      };
      return runtimeConfig;
    }
  } catch (error) {
    console.warn("Failed to fetch runtime config, using default:", error);
  }

  runtimeConfig = defaultConfig;
  return runtimeConfig;
}

export const config = {
  async getRayApiUrl(): Promise<string> {
    const cfg = await fetchRuntimeConfig();
    return `${cfg.apiserver.domain}${cfg.apiserver.rayApiPath}`;
  },

  async getCoreApiUrl(): Promise<string | undefined> {
    const cfg = await fetchRuntimeConfig();
    return cfg.apiserver.coreApiPath
      ? `${cfg.apiserver.domain}${cfg.apiserver.coreApiPath}`
      : undefined;
  },
  async getHistoryServerUrl() {
    const cfg = await fetchRuntimeConfig();
    return {
      domain: cfg.historyserver.domain,
      apiPath: cfg.historyserver.apiPath,
      proxyEndpoint: cfg.historyserver.proxyEndpoint,
    };
  },

  get rayApiUrl(): string {
    if (runtimeConfig) {
      return `${runtimeConfig.apiserver.domain}${runtimeConfig.apiserver.rayApiPath}`;
    }
    return `${defaultConfig.apiserver.domain}${defaultConfig.apiserver.rayApiPath}`;
  },

  get coreApiUrl(): string | undefined {
    if (runtimeConfig?.apiserver.coreApiPath) {
      return `${runtimeConfig.apiserver.domain}${runtimeConfig.apiserver.coreApiPath}`;
    }
    return defaultConfig.apiserver.coreApiPath
      ? `${defaultConfig.apiserver.domain}${defaultConfig.apiserver.coreApiPath}`
      : undefined;
  },
};

export const roblox = false;
