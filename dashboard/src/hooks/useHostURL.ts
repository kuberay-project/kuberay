import React from "react";

export const useHostURL = () => {
  const [hostURL, setHostURL] = React.useState("");

  React.useEffect(() => {
    const host = window.location.host;
    setHostURL(host);
  }, []);

  return { hostURL };
};
