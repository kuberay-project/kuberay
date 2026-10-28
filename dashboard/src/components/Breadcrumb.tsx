"use client";
import ChevronRightRoundedIcon from "@mui/icons-material/ChevronRightRounded";
import HomeRoundedIcon from "@mui/icons-material/HomeRounded";
import { Breadcrumbs, Typography } from "@mui/joy";
import { usePathname } from "next/navigation";
import NextLink from "next/link";

export const Breadcrumb = () => {
  const paths = usePathname();
  const pathNames = paths.split("/").filter((path) => path);

  return (
    <Breadcrumbs
      size="sm"
      aria-label="breadcrumbs"
      sx={{ pl: 0 }}
      separator={<ChevronRightRoundedIcon fontSize="small" />}
    >
      <NextLink href="/" className="flex">
        <HomeRoundedIcon />
      </NextLink>
      {/* <NextLink href="/" className="flex">
        <Typography
          color={pathNames.length === 0 ? "primary" : "neutral"}
          fontWeight={500}
          fontSize={12}
        >
          Jobs
        </Typography>
      </NextLink> */}
      {pathNames.map((link, index) => {
        const url = `/${pathNames.slice(0, index + 1).join("/")}`;
        return (
          <NextLink key={url} href={url}>
            <Typography
              color={index === pathNames.length - 1 ? "primary" : "neutral"}
              fontWeight={500}
              fontSize={12}
            >
              {link[0].toUpperCase() + link.slice(1, link.length)}
            </Typography>
          </NextLink>
        );
      })}
    </Breadcrumbs>
  );
};
