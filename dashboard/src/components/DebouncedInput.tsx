import React from "react";
import Input, { InputProps } from "@mui/joy/Input";

type DebouncedInputProps = {
  handleDebounce: (value: string) => void;
  debounceTimeout: number;
};

export default function DeboucedInput(props: InputProps & DebouncedInputProps) {
  const { handleDebounce, debounceTimeout, ...rest } = props;

  const timerRef = React.useRef<ReturnType<typeof setTimeout>>();

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => {
      handleDebounce(event.target.value);
    }, debounceTimeout);
  };

  return <Input {...rest} onChange={handleChange} />;
}
