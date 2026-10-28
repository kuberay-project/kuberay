"use client";

import React from "react";
import { useRouter } from "next/navigation";
import { Box, IconButton, Typography } from "@mui/joy";
import KeyboardBackspaceIcon from "@mui/icons-material/KeyboardBackspace";
import { HistoryTasksTable } from "@/components/HistoryTasks/HistoryTasksTable";

export default function HistoryTasksPage() {
  const router = useRouter();

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
          History Tasks
        </Typography>
      </Box>

      <HistoryTasksTable />
    </>
  );
}
