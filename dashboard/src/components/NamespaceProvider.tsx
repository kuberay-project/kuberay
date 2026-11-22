import React, { useEffect, useState } from "react";
import { fetchRuntimeConfig } from "@/utils/constants";

const NamespaceContext = React.createContext<string>("");

interface NamespaceProviderProps {
  children: React.ReactNode;
}

const NamespaceProvider: React.FC<NamespaceProviderProps> = ({ children }) => {
  const [namespace, setNamespace] = useState("default");

  useEffect(() => {
    fetchRuntimeConfig().then((cfg) => {
      if (cfg.defaultNamespace) {
        setNamespace((prev) =>
          prev === "default" ? cfg.defaultNamespace! : prev,
        );
      }
    });
  }, []);

  // This useEffect is necessary since the window object is not available
  // during static generation. After the component is rendered on browser, we can use it.
  useEffect(() => {
    // From https://github.com/kubeflow/kubeflow/tree/bd7f250df22e144b114177536309d28651b4ddbb/components/centraldashboard#client-side-library
    // @ts-ignore
    if (window.centraldashboard?.CentralDashboardEventHandler) {
      // @ts-ignore
      window.centraldashboard.CentralDashboardEventHandler.init(function (
        cdeh: any,
      ) {
        // Binds a callback that gets invoked anytime the Dashboard's
        // namespace is changed
        cdeh.onNamespaceSelected = function (namespace: string) {
          setNamespace(namespace);
        };
      });
    }
  });

  return (
    <NamespaceContext.Provider value={namespace}>
      {children}
    </NamespaceContext.Provider>
  );
};

const useNamespace = () => {
  const context = React.useContext(NamespaceContext);

  return context;
};

export { NamespaceProvider, useNamespace, NamespaceContext };
