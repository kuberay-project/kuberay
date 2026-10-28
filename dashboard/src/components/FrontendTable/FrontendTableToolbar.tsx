import React from "react";
import SearchIcon from "@mui/icons-material/Search";
import {
  Box,
  FormControl,
  FormLabel,
  Option,
  Select,
  Tab,
  TabList,
  Tabs,
  tabClasses,
} from "@mui/joy";
import ClearableSelect from "../ClearableSelect";
import DebouncedInput from "../DebouncedInput";
import { capitalize } from "../ClustersTable/ClusterStatusParser";

interface FrontendTableToolbarProps {
  setSearch: (search: string) => void;
  statusFilter: string | null;
  setStatusFilter: (statusFilter: any) => void;
  statuses: string[];
  refreshInterval: number;
  setRefreshInterval: (refreshInterval: number) => void;
  name: string;
  typeFilter?: number;
  setTypeFilter?: (typeFilter: number) => void;
  types?: string[];
}

export const FrontendTableToolbar: React.FC<FrontendTableToolbarProps> = ({
  setSearch,
  statusFilter,
  setStatusFilter,
  statuses,
  refreshInterval,
  setRefreshInterval,
  name,
  typeFilter,
  setTypeFilter,
  types,
}) => {
  const renderFilters = () => (
    <>
      <FormControl size="sm" sx={{ flex: 1 }}>
        <FormLabel>Search for {name}</FormLabel>
        <DebouncedInput
          debounceTimeout={100}
          handleDebounce={(input) => setSearch(input)}
          size="sm"
          placeholder="Search"
          startDecorator={<SearchIcon />}
        />
      </FormControl>
      <FormControl size="sm">
        <FormLabel>Status</FormLabel>
        <ClearableSelect
          size="sm"
          placeholder="Filter by status"
          slotProps={{ button: { sx: { whiteSpace: "nowrap" } } }}
          value={statusFilter}
          onChange={(e, newValue) => setStatusFilter(newValue)}
        >
          {statuses.map((status, index) => (
            <Option key={index} value={status}>
              {capitalize(status)}
            </Option>
          ))}
        </ClearableSelect>
      </FormControl>
      <FormControl size="sm" sx={{ minWidth: "inherit" }}>
        <FormLabel>Refresh</FormLabel>
        <Select
          size="sm"
          slotProps={{ button: { sx: { whiteSpace: "nowrap" } } }}
          value={refreshInterval}
          onChange={(e, newValue) => setRefreshInterval(newValue as number)}
        >
          <Option value={0}>Off</Option>
          <Option value={5000}>5s</Option>
          <Option value={30000}>30s</Option>
        </Select>
      </FormControl>
    </>
  );

  return (
    <Box
      sx={{
        py: 1,
        display: "flex",
        flexWrap: "wrap",
        gap: 3,
        justifyContent: "space-between",
      }}
    >
      {types && setTypeFilter && typeFilter !== undefined && (
        <FormControl size="sm">
          <FormLabel>
            {name.charAt(0).toUpperCase() + name.slice(1, -1)} type
          </FormLabel>
          <Tabs
            aria-label="tabs"
            value={typeFilter}
            sx={{ bgcolor: "transparent", justifyContent: "center" }}
            size="sm"
            onChange={(e, newValue) => setTypeFilter(newValue as number)}
          >
            <TabList
              disableUnderline
              size="sm"
              sx={{
                p: 0.5,
                gap: 0.5,
                borderRadius: "md",
                bgcolor: "background.level1",
                "--ListItem-minHeight": "0px",
                fontSize: "0.75rem",
                fontWeight: 500,
                [`& .${tabClasses.root}[aria-selected="true"]`]: {
                  boxShadow: "sm",
                  bgcolor: "background.surface",
                },
              }}
            >
              {types.map((type, index) => (
                <Tab key={index} disableIndicator>
                  {type}
                </Tab>
              ))}
            </TabList>
          </Tabs>
        </FormControl>
      )}
      <Box
        sx={{
          display: "flex",
          flexWrap: "wrap",
          gap: 1.5,
          flex: 1,
          "& > *": {
            minWidth: "120px",
          },
        }}
      >
        {renderFilters()}
      </Box>
    </Box>
  );
};
