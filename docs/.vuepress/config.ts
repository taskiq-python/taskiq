import { defineUserConfig } from "vuepress";
import { searchProPlugin } from "vuepress-plugin-search-pro";
import { hopeTheme } from "vuepress-theme-hope";

export default defineUserConfig({
  lang: "en-US",
  title: "Taskiq",
  description: "Distributed task queue with full async support",
  head: [
    [
      "meta",
      {
        property: "og:image",
        content: "https://taskiq-python.github.io/logo.svg",
      },
    ],
  ],

  theme: hopeTheme({
    hostname: "https://taskiq-python.github.io",
    logo: "/logo.svg",

    repo: "taskiq-python/taskiq",
    docsBranch: "master",
    docsDir: "docs",

    navbarAutoHide: "none",
    sidebar: "structure",

    pure: true,
    backToTop: false,

    plugins: {
      copyCode: {
        showInMobile: true,
      },

      mdEnhance: {
        tabs: true,
        mermaid: true,
      },

      sitemap: {
        changefreq: "daily",
        sitemapFilename: "sitemap.xml",
      },
    },
  }),

  plugins: [searchProPlugin({ indexContent: true })],
});
