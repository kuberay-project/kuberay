"use client";

import React from "react";
import { useRouter } from "next/navigation";
import { Box, IconButton, Typography } from "@mui/joy";
import KeyboardBackspaceIcon from "@mui/icons-material/KeyboardBackspace";
import { HistoryLogsPanel } from "@/components/HistoryLogs/HistoryLogsPanel";
import { LogFileViewer } from "@/components/HistoryLogs/LogFileViewer";

export default function HistoryLogsPage() {
  const router = useRouter();
  const [viewing, setViewing] = React.useState<{
    nodeId: string;
    filename: string;
  } | null>(null);

  if (viewing) {
    return (
      <LogFileViewer
        nodeId={viewing.nodeId}
        filename={viewing.filename}
        onBack={() => setViewing(null)}
      />
    );
  }

  return (
    <>
      <Box
        sx={{
          display: "flex",
          my: 0.5,
          gap: 1,
          alignItems: "center",
          ml: -0.8,
        }}
      >
        <IconButton color="neutral" size="sm" onClick={router.back}>
          <KeyboardBackspaceIcon />
        </IconButton>
        <Typography level="h3" component="h2">
          History Logs
        </Typography>
      </Box>

      <HistoryLogsPanel
        onSelectFile={(nodeId, filename) => setViewing({ nodeId, filename })}
      />
    </>
  );
}
