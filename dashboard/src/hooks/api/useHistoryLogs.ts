import useSWR from "swr";
import { historyServerFetcher } from "@/utils/fetch";
import { config } from "@/utils/constants";
import type {
  HistoryLogsResponse,
  HistoryNodesResponse,
  LogFilesByCategory,
} from "@/types/historyserver";

export const useHistoryNodes = (refreshInterval: number = 0) => {
  const { data, error, isLoading } = useSWR<HistoryNodesResponse>(
    "/nodes?view=summary",
    historyServerFetcher,
    { refreshInterval },
  );

  const nodes = (data?.data?.summary ??
    []) as HistoryNodesResponse["data"]["summary"];

  return { nodes, isLoading, error };
};

export const useHistoryLogFiles = (
  nodeId: string | null,
  refreshInterval: number = 0,
) => {
  const key = nodeId
    ? `/api/v0/logs?node_id=${encodeURIComponent(nodeId)}`
    : null;

  const { data, error, isLoading } = useSWR<HistoryLogsResponse>(
    key,
    historyServerFetcher,
    { refreshInterval },
  );

  const logFiles = (data?.data?.result ?? {}) as LogFilesByCategory;

  return { logFiles, isLoading, error };
};

export const useLogFileContent = (
  nodeId: string | null,
  filename: string | null,
  lines: number = 1000,
) => {
  const { data, error, isLoading } = useSWR<string | null>(
    nodeId && filename ? `log-content:${nodeId}:${filename}:${lines}` : null,
    async () => {
      if (!nodeId || !filename) return null;
      const proxyEndpoint = (await config.getHistoryServerUrl()).proxyEndpoint;
      const params = new URLSearchParams({
        node_id: nodeId,
        filename,
        lines: String(lines),
        filter_ansi_code: "true",
      });
      const res = await fetch(
        `${proxyEndpoint}/api/v0/logs/file?${params.toString()}`,
      );
      if (!res.ok) {
        throw new Error(`Failed to fetch log file: ${res.status}`);
      }
      // The proxy wraps text in JSON (NextResponse.json), unwrap it
      const body = await res.text();
      try {
        const parsed = JSON.parse(body);
        return typeof parsed === "string" ? parsed : body;
      } catch {
        return body;
      }
    },
    { revalidateOnFocus: false },
  );

  return { content: data ?? null, isLoading, error };
};
