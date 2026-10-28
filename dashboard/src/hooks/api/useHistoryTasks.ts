import useSWR from "swr";
import { historyServerFetcher } from "@/utils/fetch";
import type { HistoryTasksResponse } from "@/types/historyserver";

export const useHistoryTasks = (refreshInterval: number = 5000) => {
  const { data, error, isLoading, mutate } = useSWR<HistoryTasksResponse | any>(
    "/api/v0/tasks",
    historyServerFetcher,
    { refreshInterval },
  );

  const tasks = (data?.data?.result?.result ||
    []) as HistoryTasksResponse["data"]["result"]["result"];

  return {
    tasks,
    isLoading,
    error,
    mutate,
  };
};
