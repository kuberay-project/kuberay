"use client";

import { FormControl, FormLabel, Option, Select } from "@mui/joy";
import React from "react";
import { useNamespace, useSetNamespace } from "@/components/NamespaceProvider";
import { useListNamespaces } from "@/hooks/api/useListNamespaces";
import { ALL_NAMESPACES } from "@/utils/config-defaults";

// Global namespace switcher. "All namespaces" lists resources from every
// namespace; operations always use the namespace of the targeted resource.
export const NamespaceSelector = () => {
  const namespace = useNamespace();
  const setNamespace = useSetNamespace();
  const { namespaces } = useListNamespaces();
  const [visitedNamespaces, setVisitedNamespaces] = React.useState<string[]>(
    [],
  );

  // Remember specific namespaces so switching to "all" still offers a way
  // back when namespace listing is unavailable (e.g. missing RBAC).
  React.useEffect(() => {
    if (namespace && namespace !== ALL_NAMESPACES) {
      setVisitedNamespaces((prev) =>
        prev.includes(namespace) ? prev : [...prev, namespace],
      );
    }
  }, [namespace]);

  const options = Array.from(
    new Set([ALL_NAMESPACES, ...visitedNamespaces, namespace, ...namespaces]),
  );

  return (
    <FormControl size="sm">
      <FormLabel>Namespace</FormLabel>
      <Select
        size="sm"
        value={namespace}
        slotProps={{ button: { sx: { whiteSpace: "nowrap" } } }}
        onChange={(_, newValue) => {
          if (newValue !== null) {
            setNamespace(newValue);
          }
        }}
      >
        {options.map((ns) => (
          <Option key={ns} value={ns}>
            {ns === ALL_NAMESPACES ? "All namespaces" : ns}
          </Option>
        ))}
      </Select>
    </FormControl>
  );
};
