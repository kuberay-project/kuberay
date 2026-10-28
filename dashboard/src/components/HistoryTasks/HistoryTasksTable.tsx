"use client";

import React from "react";
import dayjs from "dayjs";
import { Chip } from "@mui/joy";
import { FrontendTable } from "@/components/FrontendTable/FrontendTable";
import type { HeadCell } from "@/components/FrontendTable/FrontendTableHead";
import { FrontendTableToolbar } from "@/components/FrontendTable/FrontendTableToolbar";
import { useHistoryTasks } from "@/hooks/api/useHistoryTasks";
import type { HistoryTask } from "@/types/historyserver";

type TaskRow = {
  // FrontendTable uses this as the unique row key + selection id
  name: string; // task_id
  taskId: string;
  displayName: string;
  state: string;
  type: string;
  jobId: string;
  actorId: string;
  nodeId: string;
  workerId: string;
  startTime?: number;
  endTime?: number;
  errorMessage?: string;
};

const headCells: readonly HeadCell<TaskRow>[] = [
  { id: "name", label: "Task ID", width: 200, sortable: true },
  { id: "displayName", label: "Task Name", width: 200, sortable: true },
  { id: "state", label: "Status", width: 100, sortable: true },
  { id: "type", label: "Type", width: 160, sortable: true },
  { id: "jobId", label: "Job ID", width: 100, sortable: true },
  { id: "startTime", label: "Started", width: 140, sortable: true },
  { id: "endTime", label: "Ended", width: 140, sortable: true },
];

function stateColor(
  state: string,
): "neutral" | "primary" | "success" | "warning" | "danger" {
  switch (state) {
    case "RUNNING":
      return "primary";
    case "FINISHED":
      return "success";
    case "FAILED":
      return "danger";
    default:
      return "neutral";
  }
}

function formatTaskType(type: string): string {
  switch (type) {
    case "NORMAL_TASK":
      return "Normal";
    case "ACTOR_CREATION_TASK":
      return "Actor Creation";
    case "ACTOR_TASK":
      return "Actor";
    case "DRIVER_TASK":
      return "Driver";
    case "UNKNOWN":
      return "Unknown";
    default:
      return type;
  }
}

function toTaskRow(t: HistoryTask): TaskRow {
  return {
    name: t.task_id,
    taskId: t.task_id,
    displayName: t.name || "(no name)",
    state: t.state || "UNKNOWN",
    type: t.type || "UNKNOWN",
    jobId: t.job_id || "",
    actorId: t.actor_id || "",
    nodeId: t.node_id || "",
    workerId: t.worker_id || "",
    startTime: t.start_time,
    endTime: t.end_time,
    errorMessage: t.error_message || "",
  };
}

export const HistoryTasksTable = () => {
  const [search, setSearch] = React.useState("");
  const [statusFilter, setStatusFilter] = React.useState<string | null>(null);
  const [refreshInterval, setRefreshInterval] = React.useState(5000);

  const { tasks, isLoading, error } = useHistoryTasks(refreshInterval);

  const rows = React.useMemo(() => tasks.map(toTaskRow), [tasks]);

  const filteredRows = React.useMemo(() => {
    const q = search.trim().toLowerCase();
    return rows.filter((r) => {
      if (statusFilter && r.state !== statusFilter) {
        return false;
      }
      if (!q) {
        return true;
      }
      return (
        r.taskId.toLowerCase().includes(q) ||
        r.displayName.toLowerCase().includes(q) ||
        r.jobId.toLowerCase().includes(q) ||
        r.type.toLowerCase().includes(q)
      );
    });
  }, [rows, search, statusFilter]);

  const renderRow = (row: TaskRow) => {
    return (
      <>
        <td className="truncate">{row.displayName}</td>
        <td>
          <Chip variant="soft" size="sm" color={stateColor(row.state)}>
            {row.state}
          </Chip>
        </td>
        <td>
          <Chip variant="outlined" size="sm" color="neutral">
            {formatTaskType(row.type)}
          </Chip>
        </td>
        <td className="truncate">{row.jobId || "-"}</td>
        <td>
          {row.startTime
            ? dayjs(row.startTime).format("YYYY-MM-DD HH:mm:ss")
            : ""}
        </td>
        <td>
          {row.endTime ? dayjs(row.endTime).format("YYYY-MM-DD HH:mm:ss") : ""}
        </td>
      </>
    );
  };

  return (
    <>
      <FrontendTableToolbar
        setSearch={setSearch}
        statusFilter={statusFilter}
        setStatusFilter={setStatusFilter}
        statuses={["RUNNING", "FINISHED", "FAILED", "PENDING", "UNKNOWN"]}
        refreshInterval={refreshInterval}
        setRefreshInterval={setRefreshInterval}
        name="tasks"
      />

      <FrontendTable<TaskRow>
        data={filteredRows}
        isLoading={isLoading}
        error={error}
        headCells={headCells}
        deleteItems={async () => {
          return;
        }}
        deleting={false}
        renderRow={renderRow}
        defaultOrderBy="endTime"
        name="tasks"
        disableSelection={true}
      />
    </>
  );
};
