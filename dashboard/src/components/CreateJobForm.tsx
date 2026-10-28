"use client";

import React from "react";
import { useRouter } from "next/navigation";
import {
  FormLabel,
  Stack,
  FormControl,
  Input,
  Typography,
  Divider,
  Box,
  Button,
  Stepper,
  Step,
  StepIndicator,
  AccordionGroup,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  accordionClasses,
  FormHelperText,
  Alert,
} from "@mui/joy";
import { InfoOutlined } from "@mui/icons-material";
import { useCreateJob } from "@/hooks/api/useCreateJob";
import { CreateRayJobConfig } from "@/types/v2/api/rayjob";

export const CreateJobForm = () => {
  const router = useRouter();
  const [jobName, setJobName] = React.useState(
    "test-job" + Math.floor(Math.random() * 100),
  );
  const [dockerImage, setDockerImage] = React.useState("rayproject/ray:2.46.0");
  const [entrypoint, setEntrypoint] = React.useState(
    'python -c "import time; time.sleep(10)"',
  );

  // Head node resources
  const [headCpu, setHeadCpu] = React.useState("2");
  const [headMemory, setHeadMemory] = React.useState("4Gi");
  const [headGpu, setHeadGpu] = React.useState("0");

  // Worker node resources
  const [workerReplicas, setWorkerReplicas] = React.useState("1");
  const [workerMinReplicas, setWorkerMinReplicas] = React.useState("0");
  const [workerMaxReplicas, setWorkerMaxReplicas] = React.useState("2");
  const [workerCpu, setWorkerCpu] = React.useState("2");
  const [workerMemory, setWorkerMemory] = React.useState("4Gi");
  const [workerGpu, setWorkerGpu] = React.useState("0");

  const { creating, createJob } = useCreateJob();

  // Check if all resource fields are filled
  const areResourcesFilled = React.useMemo(() => {
    return !!(
      headCpu &&
      headMemory &&
      headGpu &&
      workerReplicas &&
      workerMinReplicas &&
      workerMaxReplicas &&
      workerCpu &&
      workerMemory &&
      workerGpu
    );
  }, [
    headCpu,
    headMemory,
    headGpu,
    workerReplicas,
    workerMinReplicas,
    workerMaxReplicas,
    workerCpu,
    workerMemory,
    workerGpu,
  ]);

  const navigateToJobsPage = () => {
    router.push("/jobs");
  };

  const handleCreateJob = async () => {
    const jobConfig: CreateRayJobConfig = {
      jobName,
      dockerImage,
      entrypoint,
      headResources: {
        cpu: headCpu,
        memory: headMemory,
        gpu: headGpu,
      },
      workerResources: {
        replicas: parseInt(workerReplicas) || 1,
        minReplicas: parseInt(workerMinReplicas) || 0,
        maxReplicas: parseInt(workerMaxReplicas) || 2,
        cpu: workerCpu,
        memory: workerMemory,
        gpu: workerGpu,
      },
    };

    const success = await createJob(jobConfig);
    if (success) {
      navigateToJobsPage();
    }
  };

  const handleCancel = () => {
    navigateToJobsPage();
  };

  return (
    <>
      <Stepper
        orientation="vertical"
        sx={{
          "--Stepper-verticalGap": "1rem",
          "--Step-gap": ".5rem",
          "--StepIndicator-size": "1.7rem",
        }}
      >
        <Step
          indicator={
            <StepIndicator
              variant={jobName ? "solid" : "outlined"}
              color="primary"
            >
              1
            </StepIndicator>
          }
        >
          <Typography level="title-lg">Details</Typography>
          <FormControl size="sm">
            <FormLabel>Name</FormLabel>
            <Input
              placeholder="ex: my-rayjob"
              value={jobName}
              onChange={(e) => setJobName(e.target.value)}
            />
          </FormControl>
        </Step>
        <Step
          indicator={
            <StepIndicator
              variant={dockerImage && entrypoint ? "solid" : "outlined"}
              color="primary"
            >
              2
            </StepIndicator>
          }
        >
          <Typography level="title-lg">Environment</Typography>
          <Box sx={{ mb: 1 }}>
            <Typography level="body-sm" sx={{ mt: -1, mb: 1 }}>
              Docker image with your code and packages
            </Typography>
            <FormControl
              size="sm"
              sx={{ mb: 1.5 }}
              error={dockerImage.startsWith("http")}
            >
              <FormLabel>Docker Image</FormLabel>
              <Input
                placeholder="ex: docker.artifactory.rbx.com/your-image-with-code"
                value={dockerImage}
                onChange={(e) => setDockerImage(e.target.value)}
              />
              {dockerImage.startsWith("http") && (
                <FormHelperText>
                  <InfoOutlined />
                  Docker image should not start with http.
                </FormHelperText>
              )}
            </FormControl>
            <FormControl size="sm">
              <FormLabel>Entrypoint</FormLabel>
              <Input
                placeholder="ex: python /path/to/driver.py"
                value={entrypoint}
                onChange={(e) => setEntrypoint(e.target.value)}
              />
            </FormControl>
          </Box>
        </Step>
        <Step
          indicator={
            <StepIndicator
              variant={areResourcesFilled ? "solid" : "outlined"}
              color="primary"
            >
              3
            </StepIndicator>
          }
        >
          <Typography level="title-lg">Hardware Resources</Typography>
          <Typography level="body-sm" sx={{ mt: -1, mb: 1 }}>
            Configure compute resources for your Ray cluster
          </Typography>
          {!areResourcesFilled && (
            <Alert color="warning" size="sm" sx={{ mb: 1 }}>
              <InfoOutlined />
              Please fill in all resource fields
            </Alert>
          )}
          <AccordionGroup
            disableDivider
            sx={{
              mt: 2,
              [`& .${accordionClasses.root}`]: {
                "& button": {
                  borderRadius: "5px",
                },
              },
            }}
          >
            <Accordion sx={{ mb: 1 }} defaultExpanded>
              <AccordionSummary>Head Node</AccordionSummary>
              <AccordionDetails sx={{ mt: 0.5 }}>
                <Divider />
                <Typography level="title-md" mt={1.5}>
                  Resources
                </Typography>
                <Stack direction="row" gap={2} mt={0.5} mb={2} flexWrap="wrap">
                  <FormControl
                    size="sm"
                    sx={{ flex: "1 1 150px", minWidth: "150px" }}
                  >
                    <FormLabel>CPU Cores</FormLabel>
                    <Input
                      value={headCpu}
                      onChange={(e) => setHeadCpu(e.target.value)}
                    />
                  </FormControl>
                  <FormControl
                    size="sm"
                    sx={{ flex: "1 1 150px", minWidth: "150px" }}
                  >
                    <FormLabel>Memory</FormLabel>
                    <Input
                      value={headMemory}
                      onChange={(e) => setHeadMemory(e.target.value)}
                    />
                  </FormControl>
                  <FormControl
                    size="sm"
                    sx={{ flex: "1 1 150px", minWidth: "150px" }}
                  >
                    <FormLabel>GPU</FormLabel>
                    <Input
                      value={headGpu}
                      onChange={(e) => setHeadGpu(e.target.value)}
                    />
                  </FormControl>
                </Stack>
              </AccordionDetails>
            </Accordion>
            <Accordion defaultExpanded>
              <AccordionSummary>Worker Nodes</AccordionSummary>
              <AccordionDetails sx={{ mt: 0.5 }}>
                <Divider />
                <Typography level="title-md" mt={1.5}>
                  Replicas
                </Typography>
                <Stack direction="row" gap={2} mt={0.5} flexWrap="wrap">
                  <FormControl
                    size="sm"
                    sx={{ flex: "1 1 150px", minWidth: "150px" }}
                  >
                    <FormLabel>Desired replicas</FormLabel>
                    <Input
                      type="number"
                      value={workerReplicas}
                      onChange={(e) => setWorkerReplicas(e.target.value)}
                    />
                  </FormControl>
                  <FormControl
                    size="sm"
                    sx={{ flex: "1 1 150px", minWidth: "150px" }}
                  >
                    <FormLabel>Min replicas</FormLabel>
                    <Input
                      type="number"
                      value={workerMinReplicas}
                      onChange={(e) => setWorkerMinReplicas(e.target.value)}
                    />
                  </FormControl>
                  <FormControl
                    size="sm"
                    sx={{ flex: "1 1 150px", minWidth: "150px" }}
                  >
                    <FormLabel>Max replicas</FormLabel>
                    <Input
                      type="number"
                      value={workerMaxReplicas}
                      onChange={(e) => setWorkerMaxReplicas(e.target.value)}
                    />
                  </FormControl>
                </Stack>
                <Typography level="title-md" mt={1.5}>
                  Resources
                </Typography>
                <Stack direction="row" gap={2} mt={0.5} mb={2} flexWrap="wrap">
                  <FormControl
                    size="sm"
                    sx={{ flex: "1 1 150px", minWidth: "150px" }}
                  >
                    <FormLabel>CPU Cores</FormLabel>
                    <Input
                      value={workerCpu}
                      onChange={(e) => setWorkerCpu(e.target.value)}
                    />
                  </FormControl>
                  <FormControl
                    size="sm"
                    sx={{ flex: "1 1 150px", minWidth: "150px" }}
                  >
                    <FormLabel>Memory</FormLabel>
                    <Input
                      value={workerMemory}
                      onChange={(e) => setWorkerMemory(e.target.value)}
                    />
                  </FormControl>
                  <FormControl
                    size="sm"
                    sx={{ flex: "1 1 150px", minWidth: "150px" }}
                  >
                    <FormLabel>GPU</FormLabel>
                    <Input
                      value={workerGpu}
                      onChange={(e) => setWorkerGpu(e.target.value)}
                    />
                  </FormControl>
                </Stack>
              </AccordionDetails>
            </Accordion>
          </AccordionGroup>
        </Step>
      </Stepper>
      <Stack direction="row" gap={2} mt={5} ml={4.5}>
        <Button
          className={"bg-[#0B6BCB]"}
          disabled={
            !(jobName && dockerImage && entrypoint && areResourcesFilled)
          }
          onClick={handleCreateJob}
          loading={creating}
        >
          Create Job
        </Button>
        <Button variant="outlined" color="neutral" onClick={handleCancel}>
          Cancel
        </Button>
      </Stack>
    </>
  );
};
