"use client";
import { ClustersTable } from "@/components/ClustersTable/ClustersTable";
import { Box, Typography } from "@mui/joy";

export default function ClustersPage() {
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
          Clusters
        </Typography>
        <Box sx={{ display: "flex" }}>
          {/* <Button
            variant="outlined"
            color="primary"
            size="sm"
            startDecorator={<AddRounded />}
            component={NextLink}
            href="/clusters/new"
          >
            Create Cluster
          </Button> */}
        </Box>
      </Box>
      <ClustersTable />
    </>
  );
}
