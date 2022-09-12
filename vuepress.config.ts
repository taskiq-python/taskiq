import { defineUserConfig } from 'vuepress'
import { searchPlugin } from '@vuepress/plugin-search'
import { hopeTheme, sidebar } from "vuepress-theme-hope";

export default defineUserConfig({
    lang: 'en-US',
    title: "Taskiq",
    description: 'Distributed task queue with full async support',
    head: [
        ['meta', { property: 'og:image', content: 'https://taskiq-python.github.io/logo.svg' }]
    ],
    theme: hopeTheme({
        logo: "/logo.svg",
        pure: true,
        backToTop: false,
        repo: 'taskiq-python/taskiq',
        editLinkPattern: ":repo/edit/master/docs/:path",
        plugins: {
            seo: {},
            sitemap: {
                hostname: 'taskiq-python.github.io',
                changefreq: 'daily',
                sitemapFilename: 'sitemap.xml',
            },
            copyCode: { showInMobile: true, pure: true },
            mdEnhance: {
                tabs: true,
                mermaid: true
            }
        },
        sidebar: "structure",
        navbarAutoHide: "none",
    }),
    plugins: [searchPlugin()]
});
