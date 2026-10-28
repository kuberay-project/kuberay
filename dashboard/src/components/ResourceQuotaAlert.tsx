"use client";

import { useSnackBar } from "@/components/SnackBarProvider";
import { useHostURL } from "@/hooks/useHostURL";
import Link from "@mui/joy/Link";
import React from "react";
import { useNamespace } from "./NamespaceProvider";
import { JobRow } from "@/types/table";

interface ResourceQuotaAlertProps {
  jobs: JobRow[];
}

// This component monitors the current list of jobs for resource quota errors
// and shows an alert explaining the action the user should take.
// It returns nothing, but it can't be a hook since it creates jsx element for
// the alert message.
export const ResourceQuotaAlert: React.FC<ResourceQuotaAlertProps> = ({
  jobs,
}) => {
  // Only show alert once per page load
  const [showedAlert, setShowedAlert] = React.useState(false);
  const snackBar = useSnackBar();
  const namespace = useNamespace();
  const { hostURL } = useHostURL();

  React.useEffect(() => {
    if (!showedAlert) {
      const resourceQuotaExceeded = jobs.some((job) =>
        job.message?.includes("exceeded quota"),
      );
      if (resourceQuotaExceeded) {
        setShowedAlert(true);
        // Show alert
        snackBar.showSnackBar(
          "Resource quota exceeded",
          <>Your job is pending due to a resource quota error.</>,
          "danger",
        );
      }
    }
  }, [jobs, showedAlert, snackBar]);
  return null;
};
