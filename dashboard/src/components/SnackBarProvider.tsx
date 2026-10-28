import BlockIcon from "@mui/icons-material/Block";
import CheckRoundedIcon from "@mui/icons-material/CheckRounded";
import CloseIcon from "@mui/icons-material/Close";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { IconButton, Link, Typography } from "@mui/joy";
import Snackbar, { SnackbarCloseReason } from "@mui/joy/Snackbar";
import React, { createContext, useContext } from "react";

type SnackBarTypes = "success" | "danger" | "warning";

type SnackBarContextActions = {
  showSnackBar: (
    title: string,
    message: React.ReactNode,
    type: SnackBarTypes,
  ) => void;
};

const SnackBarContext = createContext({} as SnackBarContextActions);

interface SnackBarContextProviderProps {
  children: React.ReactNode;
}

const SnackBarProvider: React.FC<SnackBarContextProviderProps> = ({
  children,
}) => {
  const [open, setOpen] = React.useState<boolean>(false);
  const [message, setMessage] = React.useState<React.ReactNode>("");
  const [title, setTitle] = React.useState<string>("");
  const [type, setType] = React.useState<SnackBarTypes>("success");

  const showSnackBar = (
    title: string,
    message: React.ReactNode,
    type: SnackBarTypes,
  ) => {
    setMessage(message);
    setTitle(title);
    setType(type);
    setOpen(true);
  };

  const handleClose = (
    event: Event | React.SyntheticEvent<any, Event> | null,
    reason: SnackbarCloseReason,
  ) => {
    // Don't allow click away if it's a danger message
    if (reason === "clickaway" && type === "danger") {
      return;
    }
    setOpen(false);
  };

  return (
    <SnackBarContext.Provider value={{ showSnackBar }}>
      <Snackbar
        open={open}
        onClose={handleClose}
        autoHideDuration={type == "success" ? 3000 : null}
        variant="soft"
        color={type}
        startDecorator={
          {
            success: <CheckRoundedIcon />,
            warning: <InfoOutlinedIcon />,
            danger: <BlockIcon />,
          }[type]
        }
        endDecorator={
          type !== "success" ? (
            <IconButton color={type} onClick={() => setOpen(false)}>
              <CloseIcon />
            </IconButton>
          ) : null
        }
      >
        <div className="max-w-96">
          <Typography level="title-md" sx={{ color: "inherit" }}>
            {title}
          </Typography>
          <Typography level="body-sm" sx={{ color: "inherit", opacity: 0.6 }}>
            {message}
          </Typography>
        </div>
      </Snackbar>
      {children}
    </SnackBarContext.Provider>
  );
};

const useSnackBar = (): SnackBarContextActions => {
  const context = useContext(SnackBarContext);

  if (!context) {
    throw new Error("useSnackBar must be used within a SnackBarProvider");
  }

  return context;
};

export { SnackBarProvider, useSnackBar };
