import { mutate } from "swr";
import React from "react";
import { useSnackBar } from "@/components/SnackBarProvider";
import { useNamespace } from "@/components/NamespaceProvider";
import { config } from "@/utils/constants";

async function _deleteJob(namespace: string, jobName: string) {
  const baseUrl = `${config.rayApiUrl}/namespaces/${namespace}/rayjobs/`;
  const response = await fetch(`${baseUrl}${jobName}`, {
    method: "DELETE",
  });
  if (response.ok) {
    return await response.json();
  }
  console.log(response.status, response.statusText);
  const err = await response.json();
  throw new Error(err.message);
}

export const useDeleteJobs = () => {
  const [deleting, setDeleting] = React.useState(false);
  const namespace = useNamespace();
  const snackBar = useSnackBar();

  const deleteJobs = async (jobs: readonly string[]) => {
    setDeleting(true);
    // Delete the job and use "mutate" to force refresh the cache.
    // We don't want to use the result of deletion to populate cache, since the
    // response is empty.
    try {
      await mutate(
        `/namespaces/${namespace}/rayjobs`,
        Promise.all(jobs.map((jobName) => _deleteJob(namespace, jobName))),
        { populateCache: false },
      );
      snackBar.showSnackBar(
        `${jobs.length} job${jobs.length > 1 ? "s" : ""} deleted`,
        `Deleted ${jobs.join(", ")}`,
        "success",
      );
    } catch (err) {
      snackBar.showSnackBar("Failed to delete jobs", "Error: " + err, "danger");
    }
    setDeleting(false);
  };

  return { deleting, deleteJobs };
};
