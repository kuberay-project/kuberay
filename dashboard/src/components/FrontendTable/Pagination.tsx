import React from "react";
import { usePagination, DOTS } from "@/hooks/usePagination";
import Box from "@mui/joy/Box";
import Button from "@mui/joy/Button";
import IconButton, { iconButtonClasses } from "@mui/joy/IconButton";
import KeyboardArrowLeftIcon from "@mui/icons-material/KeyboardArrowLeft";
import KeyboardArrowRightIcon from "@mui/icons-material/KeyboardArrowRight";

interface PaginationProps {
  totalCount: number;
  pageSize: number;
  siblingCount?: number;
  currentPage: number;
  onPageChange: (page: number) => void;
}

export const Pagination = (props: PaginationProps) => {
  const {
    totalCount,
    pageSize,
    siblingCount = 2,
    currentPage,
    onPageChange,
  } = props;

  const paginationRange = usePagination({
    currentPage,
    totalCount,
    siblingCount,
    pageSize,
  }) as string[];

  const onNext = () => {
    onPageChange(currentPage + 1);
  };

  const onPrevious = () => {
    onPageChange(currentPage - 1);
  };

  if (paginationRange.length < 2) {
    return null;
  }

  // force TS to recognize that last item must be number
  const lastPage = paginationRange[
    paginationRange.length - 1
  ] as unknown as number;
  return (
    <Box
      sx={{
        pt: 2,
        gap: 1,
        [`& .${iconButtonClasses.root}`]: { borderRadius: "50%" },
        display: "flex",
      }}
    >
      <Button
        size="sm"
        variant="outlined"
        color="neutral"
        startDecorator={<KeyboardArrowLeftIcon />}
        disabled={currentPage === 1}
        onClick={onPrevious}
      >
        Previous
      </Button>
      <Box sx={{ flex: 1 }} />
      {paginationRange.map((pageNum, i) => {
        // currentPage is a number and pageNum is a number or string (...)
        // using loose equality to make typescript happy
        const isSelected = currentPage.toString() == pageNum;
        return (
          <IconButton
            key={i} // we don't have a better key since the "..." are duplicated
            size="sm"
            variant={
              Number(pageNum) ? (isSelected ? "soft" : "outlined") : "plain"
            }
            className={isSelected ? "bg-slate-200" : "bg-transparent"}
            disabled={!Number(pageNum)}
            color="neutral"
            onClick={() => onPageChange(Number(pageNum))}
          >
            {pageNum}
          </IconButton>
        );
      })}
      <Box sx={{ flex: 1 }} />
      <Button
        size="sm"
        variant="outlined"
        color="neutral"
        endDecorator={<KeyboardArrowRightIcon />}
        onClick={onNext}
        disabled={currentPage === lastPage}
      >
        Next
      </Button>
    </Box>
  );
};
