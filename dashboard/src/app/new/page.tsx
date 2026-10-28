"use client";
import { CreateJobForm } from "@/components/CreateJobForm";
import { Box, IconButton, Typography } from "@mui/joy";
import KeyboardBackspaceIcon from "@mui/icons-material/KeyboardBackspace";
import { useRouter } from "next/navigation";

export default function NewPage() {
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
          New Job
        </Typography>
      </Box>
      <Box maxWidth={700} sx={{ mt: 3 }}>
        <CreateJobForm />
      </Box>
    </>
  );
}
