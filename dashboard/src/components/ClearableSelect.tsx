// from https://mui.com/joy-ui/react-select/#common-examples
import { CloseRounded } from "@mui/icons-material";
import { IconButton } from "@mui/joy";
import Select, { SelectProps, SelectStaticProps } from "@mui/joy/Select";
import React from "react";

// ClearableSelect is a single select component that offers a clear button
export default function ClearableSelect<D extends React.ElementType = "button">(
  props: SelectProps<string, false, D>,
) {
  const action: SelectStaticProps["action"] = React.useRef(null);
  return (
    <Select
      {...props}
      action={action}
      {...(props.value && {
        endDecorator: (
          <IconButton
            onMouseDown={(event) => {
              // don't open the popup when clicking on this button
              event.stopPropagation();
            }}
            onClick={() => {
              props.onChange?.(null, null);
              action.current?.focusVisible();
            }}
          >
            <CloseRounded />
          </IconButton>
        ),
        indicator: null,
      })}
    />
  );
}
