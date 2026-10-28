import { mutate } from "swr";
import React from "react";
import { useSnackBar } from "@/components/SnackBarProvider";
import { useNamespace } from "@/components/NamespaceProvider";
import { config } from "@/utils/constants";

async function _deleteCluster(namespace: string, clusterName: string) {
  const baseUrl = `${config.rayApiUrl}/namespaces/${namespace}/rayclusters/`;
  const response = await fetch(`${baseUrl}${clusterName}`, {
    method: "DELETE",
  });
  if (response.ok) {
    return await response.json();
  }
  console.log(response.status, response.statusText);
  const err = await response.json();
  throw new Error(err.message);
}

export const useDeleteClusters = () => {
  const [deleting, setDeleting] = React.useState(false);
  const namespace = useNamespace();
  const snackBar = useSnackBar();

  const deleteClusters = async (clusters: readonly string[]) => {
    setDeleting(true);
    // Delete the cluster and use "mutate" to force refresh the cache.
    // We don't want to use the result of deletion to populate cache, since the
    // response is empty.
    try {
      await mutate(
        `/namespaces/${namespace}/rayclusters`,
        Promise.all(
          clusters.map((clusterName) => _deleteCluster(namespace, clusterName)),
        ),
        { populateCache: false },
      );
      snackBar.showSnackBar(
        `${clusters.length} cluster${clusters.length > 1 ? "s" : ""} deleted`,
        `Deleted ${clusters.join(", ")}`,
        "success",
      );
    } catch (err) {
      snackBar.showSnackBar(
        "Failed to delete clusters",
        "Error: " + err,
        "danger",
      );
    }
    setDeleting(false);
  };

  return { deleting, deleteClusters };
};
