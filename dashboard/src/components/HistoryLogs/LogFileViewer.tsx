"use client";

import React from "react";
import {
  Box,
  Button,
  FormControl,
  FormLabel,
  IconButton,
  Option,
  Select,
  Sheet,
  Skeleton,
  Typography,
} from "@mui/joy";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import { useLogFileContent } from "@/hooks/api/useHistoryLogs";

const SKELETON_LINE_WIDTHS = [78, 92, 64, 86, 70, 95, 82, 68, 88, 74, 90, 66];

interface Props {
  nodeId: string;
  filename: string;
  onBack: () => void;
}

export const LogFileViewer: React.FC<Props> = ({
  nodeId,
  filename,
  onBack,
}) => {
  const [lines, setLines] = React.useState(1000);
  const { content, isLoading, error } = useLogFileContent(
    nodeId,
    filename,
    lines,
  );
  const [copied, setCopied] = React.useState(false);

  const handleCopy = async () => {
    if (!content) return;
    try {
      await navigator.clipboard.writeText(content);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // clipboard access might be blocked
    }
  };

  const lineCount = React.useMemo(() => {
    if (!content) return 0;
    return content.split("\n").length;
  }, [content]);

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
      {/* Header */}
      <Box
        sx={{ display: "flex", alignItems: "center", gap: 1, flexWrap: "wrap" }}
      >
        <IconButton size="sm" variant="plain" onClick={onBack}>
          <ArrowBackIcon />
        </IconButton>
        <Typography level="title-md" noWrap sx={{ flex: 1, minWidth: 0 }}>
          {filename}
        </Typography>
        <Typography level="body-xs" textColor="neutral.400">
          Node: {nodeId.slice(0, 12)}…
        </Typography>
      </Box>

      {/* Controls */}
      <Box
        sx={{
          display: "flex",
          gap: 1.5,
          alignItems: "flex-end",
          flexWrap: "wrap",
        }}
      >
        <FormControl size="sm" sx={{ minWidth: 120 }}>
          <FormLabel>Lines</FormLabel>
          <Select size="sm" value={lines} onChange={(_, v) => v && setLines(v)}>
            <Option value={100}>100</Option>
            <Option value={500}>500</Option>
            <Option value={1000}>1,000</Option>
            <Option value={5000}>5,000</Option>
            <Option value={-1}>All</Option>
          </Select>
        </FormControl>

        <Button
          size="sm"
          variant="outlined"
          color="neutral"
          startDecorator={<ContentCopyIcon sx={{ fontSize: 14 }} />}
          onClick={handleCopy}
          disabled={!content}
        >
          {copied ? "Copied" : "Copy"}
        </Button>

        {content && (
          <Typography level="body-xs" textColor="neutral.400" sx={{ pb: 0.5 }}>
            {lineCount} line{lineCount !== 1 ? "s" : ""}
          </Typography>
        )}
      </Box>

      {/* Content */}
      <Sheet
        variant="outlined"
        sx={{
          borderRadius: "sm",
          overflow: "auto",
          maxHeight: "calc(100vh - 260px)",
          minHeight: 200,
        }}
      >
        {error ? (
          <Box sx={{ p: 2 }}>
            <Typography level="body-sm" color="danger">
              Failed to load log file{error.message ? `: ${error.message}` : ""}
            </Typography>
          </Box>
        ) : isLoading ? (
          <Box sx={{ p: 2 }}>
            {SKELETON_LINE_WIDTHS.map((width, i) => (
              <Skeleton
                key={i}
                animation="wave"
                variant="text"
                width={`${width}%`}
                sx={{ mb: 0.5 }}
              />
            ))}
          </Box>
        ) : !content ? (
          <Box sx={{ p: 2 }}>
            <Typography level="body-sm" color="neutral">
              Log file is empty.
            </Typography>
          </Box>
        ) : (
          <Box
            component="pre"
            sx={{
              m: 0,
              p: 1.5,
              fontSize: "0.75rem",
              fontFamily: "monospace",
              lineHeight: 1.6,
              whiteSpace: "pre-wrap",
              wordBreak: "break-all",
            }}
          >
            {content}
          </Box>
        )}
      </Sheet>
    </Box>
  );
};
