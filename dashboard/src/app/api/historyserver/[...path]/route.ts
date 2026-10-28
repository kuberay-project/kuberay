import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/utils/config-server";

// Proxy requests to the history server
async function proxyRequest(
  request: NextRequest,
  params: { path: string[] },
): Promise<NextResponse> {
  const config = getServerConfig();
  const historyServerUrl = config.historyserver.domain;
  // Ex:
  // /api/historyserver/clusters       → params.path = ["clusters"]
  // /api/historyserver/api/v0/tasks   → params.path = ["api", "v0", "tasks"]
  const pathSegments = params.path;

  // Extract path segments from the URL.
  // ["clusters"] -> "/clusters"
  const targetPath = "/" + pathSegments.join("/");
  const searchParams = request.nextUrl.searchParams.toString();
  // "http://localhost:8080/clusters"
  const url = `${historyServerUrl}${targetPath}${searchParams ? `?${searchParams}` : ""}`;

  try {
    const headers: HeadersInit = {};
    // Forward relevant headers
    if (request.headers.get("content-type")) {
      headers["Content-Type"] = request.headers.get("content-type")!;
    }
    // Forward cookies to history server
    if (request.headers.get("cookie")) {
      headers["Cookie"] = request.headers.get("cookie")!;
    }

    const fetchOptions: RequestInit = {
      method: request.method,
      headers,
    };

    // Forward body for POST/PUT/PATCH requests
    if (["POST", "PUT", "PATCH"].includes(request.method)) {
      const body = await request.text();
      if (body) {
        fetchOptions.body = body;
      }
    }

    const response = await fetch(url, fetchOptions);
    const data = await response.text();

    // Try to parse as JSON, otherwise return as text
    let responseData;
    try {
      responseData = JSON.parse(data);
    } catch {
      responseData = data;
    }

    const nextResponse = NextResponse.json(responseData, {
      status: response.status,
    });

    // Forward Set-Cookie headers from history server to browser
    const setCookieHeaders = response.headers.getSetCookie();
    for (const cookie of setCookieHeaders) {
      nextResponse.headers.append("Set-Cookie", cookie);
    }

    return nextResponse;
  } catch (error) {
    console.error("History server proxy error:", error);
    return NextResponse.json(
      { error: "Failed to connect to history server" },
      { status: 502 },
    );
  }
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const resolvedParams = await params;
  return proxyRequest(request, resolvedParams);
}

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const resolvedParams = await params;
  return proxyRequest(request, resolvedParams);
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const resolvedParams = await params;
  return proxyRequest(request, resolvedParams);
}

export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const resolvedParams = await params;
  return proxyRequest(request, resolvedParams);
}

export async function PATCH(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const resolvedParams = await params;
  return proxyRequest(request, resolvedParams);
}
