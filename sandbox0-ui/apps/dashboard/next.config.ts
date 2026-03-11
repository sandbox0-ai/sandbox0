import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  basePath: "/dashboard",
  transpilePackages: ["@sandbox0/ui"],
};

export default nextConfig;
