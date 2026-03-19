// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightBlog from 'starlight-blog';
import starlightChangelogs from 'starlight-changelogs';
import starlightThemeRapide from 'starlight-theme-rapide';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			expressiveCode: {
				shiki: {
					langAlias: { redis: 'text' },
				},
			},
			plugins: [
				starlightThemeRapide(),
				starlightBlog({
					title: 'Blog',
					navigation: 'none',
					authors: {
						nathan: {
							name: 'Nathan',
						},
					},
				}),
				starlightChangelogs(),
			],
			title: 'FrogDB',
			logo: {
				src: './src/assets/frogdb-logo.png',
				alt: 'FrogDB Logo',
			},
			description: 'A Redis-compatible in-memory database with persistence',
			defaultLocale: 'root',
			locales: {
				root: {
					label: 'English',
					lang: 'en',
				},
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/frogdb/frogdb' },
			],
			editLink: {
				baseUrl: 'https://github.com/frogdb/frogdb/edit/main/docs-site/',
			},
			customCss: [
				'./src/styles/custom.css',
			],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Installation', slug: 'getting-started/installation' },
						{ label: 'Quickstart', slug: 'getting-started/quickstart' },
						{ label: 'Configuration', slug: 'getting-started/configuration' },
					],
				},
				{
					label: 'Architecture',
					collapsed: true,
					autogenerate: { directory: 'architecture' },
				},
				{
					label: 'Guides',
					collapsed: true,
					autogenerate: { directory: 'guides' },
				},
				{
					label: 'Operations',
					collapsed: true,
					autogenerate: { directory: 'operations' },
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Commands', slug: 'reference/commands' },
						{ label: 'Configuration', slug: 'reference/configuration' },
						{ label: 'CLI Options', slug: 'reference/cli' },
					],
				},
				{
					label: 'Compatibility',
					items: [
						{ label: 'Redis Differences', slug: 'compatibility/redis-differences' },
						{ label: 'Migration Guide', slug: 'compatibility/migration-guide' },
					],
				},
			],
		}),
	],
});
