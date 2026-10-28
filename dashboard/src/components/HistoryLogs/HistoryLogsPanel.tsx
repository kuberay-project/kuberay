"use client";

import React from "react";
import {
  Box,
  Button,
  Chip,
  FormControl,
  FormLabel,
  Option,
  Select,
  Sheet,
  Skeleton,
  Table,
  Typography,
} from "@mui/joy";
import DescriptionIcon from "@mui/icons-material/Description";
import {
  useHistoryNodes,
  useHistoryLogFiles,
} from "@/hooks/api/useHistoryLogs";
import type { HistoryNodeSummary } from "@/types/historyserver";

// Display order for log categories – most useful ones first
const CATEGORY_ORDER = [
  "worker_out",
  "worker_err",
  "core_worker",
  "driver",
  "raylet",
  "gcs_server",
  "agent",
  "dashboard",
  "autoscaler",
  "internal",
];

const CATEGORY_LABELS: Record<string, string> = {
  worker_out: "Worker (stdout)",
  worker_err: "Worker (stderr)",
  core_worker: "Core Worker",
  driver: "Driver",
  raylet: "Raylet",
  gcs_server: "GCS Server",
  agent: "Agent",
  dashboard: "Dashboard",
  autoscaler: "Autoscaler",
  internal: "Internal",
};

function categoryColor(
  cat: string,
): "primary" | "success" | "warning" | "danger" | "neutral" {
  switch (cat) {
    case "worker_out":
    case "worker_err":
      return "primary";
    case "raylet":
    case "gcs_server":
      return "warning";
    case "driver":
      return "success";
    default:
      return "neutral";
  }
}

function nodeLabel(node: HistoryNodeSummary): string {
  const ip = node.ip || "unknown";
  const id = node.raylet?.nodeId?.slice(0, 8) || "?";
  const hostname = node.hostname || "";
  if (hostname) {
    return `${hostname} (${ip}) [${id}…]`;
  }
  return `${ip} [${id}…]`;
}

interface Props {
  onSelectFile: (nodeId: string, filename: string) => void;
}

export const HistoryLogsPanel: React.FC<Props> = ({ onSelectFile }) => {
  const [selectedNodeId, setSelectedNodeId] = React.useState<string | null>(
    null,
  );
  const [categoryFilter, setCategoryFilter] = React.useState<string | null>(
    null,
  );
  const selectedNodeIdRef = React.useRef<string | null>(null);
  const { nodes, isLoading: nodesLoading } = useHistoryNodes();
  const {
    logFiles,
    isLoading: logsLoading,
    error: logsError,
  } = useHistoryLogFiles(selectedNodeId);

  React.useEffect(() => {
    selectedNodeIdRef.current = selectedNodeId;
  }, [selectedNodeId]);

  // Auto-select a valid node when nodes load or the active cluster changes.
  React.useEffect(() => {
    const nodeIds = nodes
      .map((node) => node.raylet?.nodeId)
      .filter((id): id is string => Boolean(id));
    const currentNodeId = selectedNodeIdRef.current;

    if (currentNodeId && nodeIds.includes(currentNodeId)) {
      return;
    }

    const nextNodeId = nodeIds[0] ?? null;
    if (currentNodeId !== nextNodeId) {
      selectedNodeIdRef.current = nextNodeId;
      setSelectedNodeId(nextNodeId);
      setCategoryFilter(null);
    }
  }, [nodes]);

  // Build sorted list of (category, files)
  const sortedCategories = React.useMemo(() => {
    const entries = Object.entries(logFiles);
    const orderMap = new Map(CATEGORY_ORDER.map((c, i) => [c, i]));
    entries.sort(([a], [b]) => {
      const ia = orderMap.get(a) ?? 999;
      const ib = orderMap.get(b) ?? 999;
      return ia - ib;
    });
    if (categoryFilter) {
      return entries.filter(([cat]) => cat === categoryFilter);
    }
    return entries;
  }, [logFiles, categoryFilter]);

  const totalFiles = React.useMemo(
    () => Object.values(logFiles).reduce((s, arr) => s + arr.length, 0),
    [logFiles],
  );

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
      {/* Controls */}
      <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1.5 }}>
        <FormControl size="sm" sx={{ minWidth: 280 }}>
          <FormLabel>Node</FormLabel>
          <Select
            size="sm"
            placeholder={nodesLoading ? "Loading nodes…" : "Select a node"}
            value={selectedNodeId}
            onChange={(_, v) => {
              selectedNodeIdRef.current = v;
              setSelectedNodeId(v);
              setCategoryFilter(null);
            }}
          >
            {nodes.map((node) => {
              const id = node.raylet?.nodeId;
              return (
                <Option key={id} value={id}>
                  {nodeLabel(node)}
                </Option>
              );
            })}
          </Select>
        </FormControl>

        <FormControl size="sm" sx={{ minWidth: 160 }}>
          <FormLabel>Category</FormLabel>
          <Select
            size="sm"
            placeholder="All categories"
            value={categoryFilter}
            onChange={(_, v) => setCategoryFilter(v)}
          >
            <Option value={null}>All</Option>
            {Object.keys(logFiles).map((cat) => (
              <Option key={cat} value={cat}>
                {CATEGORY_LABELS[cat] || cat}
              </Option>
            ))}
          </Select>
        </FormControl>

        <Box sx={{ display: "flex", alignItems: "flex-end", pb: 0.5 }}>
          <Typography level="body-xs" textColor="neutral.400">
            {totalFiles} file{totalFiles !== 1 ? "s" : ""}
          </Typography>
        </Box>
      </Box>

      {/* Log files table */}
      <Sheet
        variant="outlined"
        sx={{ borderRadius: "sm", overflow: "auto", minHeight: 0 }}
      >
        <Table
          hoverRow
          stickyHeader
          sx={{
            "--TableCell-headBackground":
              "var(--joy-palette-background-level1)",
            "--Table-headerUnderlineThickness": "1px",
            "--TableRow-hoverBackground":
              "var(--joy-palette-background-level1)",
            "--TableCell-paddingY": "6px",
            "--TableCell-paddingX": "8px",
          }}
        >
          <thead>
            <tr>
              <th style={{ width: 140 }}>Category</th>
              <th>Filename</th>
              <th style={{ width: 80, textAlign: "center" }}>Action</th>
            </tr>
          </thead>
          <tbody>
            {logsError ? (
              <tr>
                <td colSpan={3} style={{ textAlign: "center" }}>
                  <Typography level="body-sm" color="danger">
                    Failed to load logs
                    {logsError.message ? `: ${logsError.message}` : ""}
                  </Typography>
                </td>
              </tr>
            ) : logsLoading || nodesLoading ? (
              Array.from({ length: 6 }).map((_, i) => (
                <tr key={i}>
                  <td>
                    <Skeleton variant="text" width={80} />
                  </td>
                  <td>
                    <Skeleton animation="wave" variant="text" width="90%" />
                  </td>
                  <td />
                </tr>
              ))
            ) : !selectedNodeId ? (
              <tr>
                <td colSpan={3} style={{ textAlign: "center" }}>
                  <Typography level="body-sm" color="neutral">
                    Select a node to view its log files.
                  </Typography>
                </td>
              </tr>
            ) : totalFiles === 0 ? (
              <tr>
                <td colSpan={3} style={{ textAlign: "center" }}>
                  <Typography level="body-sm" color="neutral">
                    No log files found for this node.
                  </Typography>
                </td>
              </tr>
            ) : (
              sortedCategories.flatMap(([category, files]) =>
                files.map((file) => (
                  <tr
                    key={`${category}-${file}`}
                    style={{ cursor: "pointer" }}
                    onClick={() =>
                      selectedNodeId && onSelectFile(selectedNodeId, file)
                    }
                  >
                    <td>
                      <Chip
                        variant="soft"
                        size="sm"
                        color={categoryColor(category)}
                      >
                        {CATEGORY_LABELS[category] || category}
                      </Chip>
                    </td>
                    <td>
                      <Box
                        sx={{ display: "flex", alignItems: "center", gap: 0.5 }}
                      >
                        <DescriptionIcon
                          sx={{ fontSize: 16, color: "neutral.400" }}
                        />
                        <Typography level="body-sm" noWrap>
                          {file}
                        </Typography>
                      </Box>
                    </td>
                    <td style={{ textAlign: "center" }}>
                      <Button
                        size="sm"
                        variant="outlined"
                        color="primary"
                        onClick={(e) => {
                          e.stopPropagation();
                          if (selectedNodeId) {
                            onSelectFile(selectedNodeId, file);
                          }
                        }}
                      >
                        View
                      </Button>
                    </td>
                  </tr>
                )),
              )
            )}
          </tbody>
        </Table>
      </Sheet>
    </Box>
  );
};
