import { defineUserConfig } from "vuepress";
import { hopeTheme } from "vuepress-theme-hope";
import { viteBundler } from '@vuepress/bundler-vite'

export default defineUserConfig({
  lang: "en-US",
  title: "Taskiq",
  description: "Async Distributed Task Manager",
  head: [
    [
      "meta",
      {
        property: "og:image",
        content: "https://taskiq-python.github.io/logo.svg",
      },
    ],
  ],

  bundler: viteBundler(),

  theme: hopeTheme({
    hostname: "https://taskiq-python.github.io",
    logo: "/logo.svg",

    repo: "taskiq-python/taskiq",
    docsBranch: "master",
    docsDir: "docs",

    navbarAutoHide: "none",
    sidebar: "structure",

    pure: true,

    plugins: {
      readingTime: false,
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
      searchPro: {
        indexContent: true,
        autoSuggestions: false,
      }
    },
  })
});
