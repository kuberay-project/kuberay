import { useSnackBar } from "@/components/SnackBarProvider";
import { useDeleteJobs } from "@/hooks/api/useDeleteJobs";
import { useListJobs } from "@/hooks/api/useListJobs";
import { JobStatus } from "@/types/v2/rayjob";
import { filterJobs } from "@/utils/filter";
import { Chip, IconButton, Tooltip } from "@mui/joy";
import { Typography } from "@mui/material";
import dayjs from "dayjs";
import Image from "next/image";
import React from "react";
import RayIcon from "../../../public/ray.png";
import GrafanaIcon from "../../../public/GrafanaIcon.svg";
import { FrontendTable } from "../FrontendTable/FrontendTable";
import { HeadCell } from "../FrontendTable/FrontendTableHead";
import { FrontendTableToolbar } from "../FrontendTable/FrontendTableToolbar";
import { ResourceQuotaAlert } from "../ResourceQuotaAlert";
import {
  getJobStatusColor,
  getJobStatusDisplay,
  getJobStatusIcon,
} from "./JobStatusParser";
import { JobRow } from "@/types/table";
import { apiVersion } from "@/utils/constants";

const headCells: readonly HeadCell<JobRow>[] = [
  {
    id: "name",
    label: "Name",
    width: 170,
    sortable: true,
  },
  {
    id: "jobStatus",
    label: "Status",
    width: 100,
    sortable: false,
  },
  {
    id: "createdAt",
    label: "Created At",
    width: 130,
    sortable: true,
  },
  {
    id: "links",
    label: "Links",
    width: 100,
    sortable: false,
  },
  {
    id: "message",
    label: "Message",
    width: 200,
    sortable: false,
  },
];

export const JobsTable = () => {
  const [search, setSearch] = React.useState("");
  const [statusFilter, setStatusFilter] = React.useState<JobStatus | null>(
    null,
  );
  const [typeFilter, setTypeFilter] = React.useState<number>(0);
  const [refreshInterval, setRefreshInterval] = React.useState(5000);

  const { jobs, isLoading, error } = useListJobs(refreshInterval, apiVersion);
  const { deleting, deleteJobs } = useDeleteJobs();

  const snackBar = useSnackBar();

  const filteredItems = React.useMemo(
    // For the compatibility of v1 and v2, we need to force cast the type here.
    () => filterJobs(jobs, search, statusFilter, typeFilter) as JobRow[],
    [jobs, search, statusFilter, typeFilter],
  );

  const renderRow = (row: JobRow) => {
    return (
      <>
        <td>
          <Tooltip
            variant="outlined"
            title={`Job status: ${getJobStatusDisplay(row.jobStatus.jobStatus)}. Job deployment status: ${row.jobStatus.jobDeploymentStatus}`}
          >
            <Chip
              variant="soft"
              size="sm"
              startDecorator={getJobStatusIcon(row.jobStatus.jobStatus)}
              color={getJobStatusColor(row.jobStatus.jobStatus)}
            >
              {getJobStatusDisplay(row.jobStatus.jobStatus)}
            </Chip>
          </Tooltip>
        </td>
        <td>{dayjs(row.createdAt).format("M/D/YY HH:mm:ss")}</td>
        <td className="flex">
          {row.links.rayHeadDashboardLink && (
            <IconButton
              variant="plain"
              size="sm"
              sx={{ minHeight: "1rem", minWidth: "1rem" }}
              title="Ray Dashboard"
              href={row.links.rayHeadDashboardLink}
              target="_blank"
              component="a"
            >
              <Image
                priority
                src={RayIcon}
                alt="Ray Dashboard"
                height={26}
                width={26}
              />
            </IconButton>
          )}
          {row.links.rayGrafanaDashboardLink && (
            <IconButton
              variant="plain"
              size="sm"
              sx={{
                minHeight: "1rem",
                minWidth: "1rem",
                px: 0.5,
              }}
              title="Grafana Metrics"
              href={row.links.rayGrafanaDashboardLink}
              target="_blank"
              component="a"
            >
              <Image priority src={GrafanaIcon} alt="Grafana Metrics" />
            </IconButton>
          )}
          {row.links.logsLink && (
            <IconButton
              variant="plain"
              size="sm"
              sx={{
                minHeight: "1rem",
                minWidth: "1rem",
                px: 0.6,
              }}
              title="Loki Logs"
              href={row.links.logsLink}
              target="_blank"
              component="a"
            >
              <Typography
                sx={{
                  fontFamily: "monospace",
                  letterSpacing: -0.9,
                  fontSize: "small",
                  color: "#0b6bcb",
                  textDecoration: "underline",
                }}
              >
                Logs
              </Typography>
            </IconButton>
          )}
        </td>
        <td className="truncate">
          <Tooltip variant="outlined" title={row.message}>
            <span>{row.message}</span>
          </Tooltip>
        </td>
      </>
    );
  };

  return (
    <>
      <ResourceQuotaAlert jobs={jobs} />
      <FrontendTableToolbar
        setSearch={setSearch}
        statusFilter={statusFilter}
        setStatusFilter={setStatusFilter}
        statuses={[
          "PENDING",
          "RUNNING",
          "STOPPED",
          "SUCCEEDED",
          "FAILED",
          "UNKNOWN",
        ]}
        refreshInterval={refreshInterval}
        setRefreshInterval={setRefreshInterval}
        name="jobs"
        typeFilter={typeFilter}
        setTypeFilter={setTypeFilter}
        types={["All", "Batch API"]}
      />
      <FrontendTable<JobRow>
        data={filteredItems}
        isLoading={isLoading}
        error={error}
        headCells={headCells}
        deleteItems={deleteJobs}
        deleting={deleting}
        renderRow={renderRow}
        defaultOrderBy="createdAt"
        name="jobs"
      />
    </>
  );
};
