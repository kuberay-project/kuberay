"use client";
// Mostly from https://mui.com/joy-ui/react-table/#sort-and-selection
import { Order } from "@/types/table";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import { Checkbox } from "@mui/joy";
import Box from "@mui/joy/Box";
import Link from "@mui/joy/Link";
import { visuallyHidden } from "@mui/utils";
import React from "react";

export interface HeadCell<T> {
  id: keyof T & string;
  label: string;
  width: number;
  sortable: boolean;
}

interface FrontendTableHeadProps<T> {
  numSelected: number;
  onRequestSort: (event: React.MouseEvent<unknown>, property: keyof T) => void;
  onSelectAllClick: (event: React.ChangeEvent<HTMLInputElement>) => void;
  order: Order;
  orderBy: string;
  rowCount: number;
  headCells: readonly HeadCell<T>[];
  disableSelection?: boolean;
}

export default function FrontendTableHead<T>(props: FrontendTableHeadProps<T>) {
  const {
    numSelected,
    onRequestSort,
    onSelectAllClick,
    order,
    orderBy,
    rowCount,
    headCells,
    disableSelection = false,
  } = props;
  const createSortHandler =
    (property: keyof T & string) => (event: React.MouseEvent<unknown>) => {
      onRequestSort(event, property);
    };

  return (
    <thead>
      <tr>
        <th style={{ width: 48, textAlign: "center", padding: "12px 6px" }}>
          {!disableSelection && (
            <Checkbox
              size="sm"
              indeterminate={numSelected > 0 && numSelected < rowCount}
              checked={rowCount > 0 && numSelected === rowCount}
              onChange={onSelectAllClick}
              slotProps={{
                input: {
                  "aria-label": "select all jobs",
                },
              }}
              sx={{ verticalAlign: "text-bottom" }}
            />
          )}
        </th>
        {headCells.map((headCell) => {
          const active = orderBy === headCell.id;
          return (
            <th
              key={headCell.id}
              aria-sort={
                active
                  ? ({ asc: "ascending", desc: "descending" } as const)[order]
                  : undefined
              }
              style={{ padding: "12px 6px", width: headCell.width }}
            >
              <Link
                underline="none"
                color="neutral"
                textColor={active ? "primary.plainColor" : undefined}
                component="button"
                disabled={!headCell.sortable}
                onClick={createSortHandler(headCell.id as keyof T & string)}
                fontWeight="lg"
                endDecorator={
                  <ArrowDownwardIcon
                    sx={{ opacity: active ? 1 : 0, width: "18px" }}
                  />
                }
                sx={{
                  "& svg": {
                    transition: "0.2s",
                    transform:
                      active && order === "desc"
                        ? "rotate(0deg)"
                        : "rotate(180deg)",
                  },
                  "&:hover": { "& svg": { opacity: 1 } },
                  "&.Mui-disabled": {
                    color: "rgba(var(--joy-palette-neutral-mainChannel) / 1)",
                  },
                }}
              >
                {headCell.label}
                {active ? (
                  <Box component="span" sx={visuallyHidden}>
                    {order === "desc"
                      ? "sorted descending"
                      : "sorted ascending"}
                  </Box>
                ) : null}
              </Link>
            </th>
          );
        })}
      </tr>
    </thead>
  );
}
