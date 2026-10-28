"use client";
// Tailwind global styles have conflicts with Joy UI. For example,
// button background is overridden to be transparent.
// See https://github.com/tailwindlabs/tailwindcss/issues/7500
import { Breadcrumb } from "@/components/Breadcrumb";
import { Box } from "@mui/joy";
import { CssVarsProvider } from "@mui/joy/styles";
import Script from "next/script";
import "./globals.css";
import { SnackBarProvider } from "@/components/SnackBarProvider";
import { NamespaceProvider } from "@/components/NamespaceProvider";
import { FirstVisitProvider } from "@/components/FirstVisitContext";

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <Script strategy="beforeInteractive" src="/dashboard_lib.bundle.js" />
        <CssVarsProvider>
          <NamespaceProvider>
            <FirstVisitProvider>
              <SnackBarProvider>
                <Box component="main" sx={{ px: 4, py: 2 }}>
                  <Breadcrumb />
                  {children}
                </Box>
              </SnackBarProvider>
            </FirstVisitProvider>
          </NamespaceProvider>
        </CssVarsProvider>
      </body>
    </html>
  );
}
