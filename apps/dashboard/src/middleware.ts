import { type NextRequest, NextResponse } from "next/server";

import { dashboardAccessTokenCookieName } from "@sandbox0/dashboard-core";

// Paths that are always publicly accessible — no auth check performed.
const PUBLIC_PREFIXES = [
  "/login",
  "/api/auth/",
  "/_next/",
  "/favicon.ico",
  "/sandbox0.png",
];

function isPublicPath(pathname: string): boolean {
  return PUBLIC_PREFIXES.some(
    (prefix) => pathname === prefix || pathname.startsWith(prefix),
  );
}

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  if (isPublicPath(pathname)) {
    return NextResponse.next();
  }

  const accessToken = request.cookies.get(dashboardAccessTokenCookieName);
  if (accessToken?.value) {
    return NextResponse.next();
  }

  const loginURL = new URL("/login", request.url);
  return NextResponse.redirect(loginURL, { status: 307 });
}

export const config = {
  // Run on all routes except Next.js internals and static files.
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
