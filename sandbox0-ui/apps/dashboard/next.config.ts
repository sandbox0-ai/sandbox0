import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  basePath: "/dashboard",
  transpilePackages: ["@sandbox0/ui", "@sandbox0/dashboard-core"],
};

export default nextConfig;
