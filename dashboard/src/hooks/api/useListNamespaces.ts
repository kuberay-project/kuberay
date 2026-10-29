import useSWR from "swr";
import { coreApiFetcher } from "@/utils/fetch";

// Response shape of GET /api/v1/namespaces (Kubernetes NamespaceList)
interface NamespaceListResponse {
  items: {
    metadata: {
      name: string;
    };
  }[];
}

export const useListNamespaces = (refreshInterval: number = 60000) => {
  const { data, error, isLoading } = useSWR<NamespaceListResponse | null>(
    "/namespaces",
    coreApiFetcher,
    { refreshInterval },
  );

  const namespaces = (data?.items ?? [])
    .map((item) => item.metadata.name)
    .sort((a, b) => a.localeCompare(b));

  return { namespaces, isLoading, error };
};
