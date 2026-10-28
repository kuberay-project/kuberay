"use client";

import React from "react";
import dayjs from "dayjs";
import { useRouter } from "next/navigation";
import { Box, Button, Chip, Typography } from "@mui/joy";
import { FrontendTable } from "@/components/FrontendTable/FrontendTable";
import type { HeadCell } from "@/components/FrontendTable/FrontendTableHead";
import { FrontendTableToolbar } from "@/components/FrontendTable/FrontendTableToolbar";
import { useHistoryClusters } from "@/hooks/api/useHistoryClusters";
import { useSnackBar } from "@/components/SnackBarProvider";
import type { HistoryClusterInfo } from "@/types/historyserver";

type ClusterRow = {
  name: string; // unique key for FrontendTable selection
  namespace: string;
  sessionName: string;
  createTime?: string;
  createTimeStamp?: number;
  tasks: string;
};

const headCells: readonly HeadCell<ClusterRow>[] = [
  // NOTE: FrontendTable always renders `row.name` as the first data column.
  { id: "name", label: "Cluster Name", width: 260, sortable: true },
  { id: "namespace", label: "Namespace", width: 140, sortable: true },
  { id: "sessionName", label: "Status", width: 100, sortable: true },
  { id: "createTimeStamp", label: "Created", width: 160, sortable: true },
  { id: "tasks", label: "Actions", width: 200, sortable: false },
];

function toRow(c: HistoryClusterInfo): ClusterRow {
  const createTime = c.createTime
    ? dayjs(c.createTime).format("YYYY-MM-DD HH:mm:ss")
    : "N/A";
  return {
    name: `${c.name}-${c.namespace}-${createTime}`,
    tasks: c.name,
    namespace: c.namespace,
    sessionName: c.sessionName,
    createTime,
    createTimeStamp: c.createTimeStamp,
  };
}

export default function HistoryClustersPage() {
  const router = useRouter();
  const [search, setSearch] = React.useState("");
  const [statusFilter, setStatusFilter] = React.useState<string | null>(null);
  const [refreshInterval, setRefreshInterval] = React.useState(5000);
  const { showSnackBar } = useSnackBar();

  const { clusters, isLoading, error, enterCluster } =
    useHistoryClusters(refreshInterval);

  const rows = React.useMemo(() => clusters.map(toRow), [clusters]);

  const sessionOptions = React.useMemo(() => {
    return ["LIVE", "DEAD"];
  }, []);

  const filteredRows = React.useMemo(() => {
    const q = search.trim().toLowerCase();
    return rows.filter((r) => {
      const isLive = r.sessionName === "live";

      if (statusFilter) {
        if (statusFilter === "LIVE" && !isLive) {
          return false;
        }
        if (statusFilter === "DEAD" && isLive) {
          return false;
        }
      }

      if (!q) {
        return true;
      }
      return (
        r.tasks.toLowerCase().includes(q) ||
        r.namespace.toLowerCase().includes(q) ||
        r.sessionName.toLowerCase().includes(q)
      );
    });
  }, [rows, search, statusFilter]);

  const handleNavigate = async (row: ClusterRow, target: string) => {
    try {
      await enterCluster(row.namespace, row.tasks, row.sessionName);
      router.push(target);
    } catch (e) {
      console.error("Failed to enter cluster", e);
      showSnackBar("Error", `Failed to enter cluster: ${e}`, "danger");
    }
  };

  const renderRow = (row: ClusterRow) => {
    const isLive = row.sessionName === "live";

    return (
      <>
        <td className="truncate">
          <Chip variant="outlined" size="sm" color="success">
            {/* The first letter capitalized */}
            {row.namespace.charAt(0).toUpperCase() + row.namespace.slice(1)}
          </Chip>
        </td>
        <td className="truncate">
          <Chip variant="soft" size="sm" color={isLive ? "primary" : "neutral"}>
            {isLive ? "Live" : "Dead"}
          </Chip>
        </td>
        <td>{row.createTime}</td>
        <td>
          <Box sx={{ display: "flex", gap: 0.5 }}>
            <Button
              size="sm"
              variant="outlined"
              onClick={() => handleNavigate(row, "/history/tasks")}
            >
              Tasks
            </Button>
            <Button
              size="sm"
              variant="outlined"
              onClick={() => handleNavigate(row, "/history/logs")}
            >
              Logs
            </Button>
          </Box>
        </td>
      </>
    );
  };

  return (
    <>
      <Box
        sx={{
          display: "flex",
          my: 0.5,
          gap: 1,
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <Typography level="h2" component="h2">
          History Clusters
        </Typography>
      </Box>

      <FrontendTableToolbar
        setSearch={setSearch}
        statusFilter={statusFilter}
        setStatusFilter={setStatusFilter}
        statuses={sessionOptions}
        refreshInterval={refreshInterval}
        setRefreshInterval={setRefreshInterval}
        name="history clusters"
      />

      <FrontendTable<ClusterRow>
        data={filteredRows}
        isLoading={isLoading}
        error={error}
        headCells={headCells}
        deleteItems={async () => {
          return;
        }}
        deleting={false}
        renderRow={renderRow}
        defaultOrderBy="createTimeStamp"
        name="history clusters"
        disableSelection={true}
      />
    </>
  );
}
