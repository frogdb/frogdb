// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightChangelogs from 'starlight-changelogs';
import starlightThemeRapide from 'starlight-theme-rapide';
import starlightLinksValidator from 'starlight-links-validator';
import starlightSidebarTopics from 'starlight-sidebar-topics';
import { starlightIconsPlugin } from 'starlight-plugin-icons';
import remarkBaseUrl from './plugins/remark-base-url.mjs';

const BASE = '/frogdb';

// https://astro.build/config
export default defineConfig({
	site: 'https://frogdb.github.io',
	base: BASE,
	markdown: {
		remarkPlugins: [[remarkBaseUrl, { base: BASE }]],
	},
	integrations: [
		starlight({
			expressiveCode: {
				shiki: {
					langAlias: { redis: 'text' },
				},
			},
			plugins: [
				starlightThemeRapide(),
				starlightIconsPlugin(),
				starlightLinksValidator({
					errorOnRelativeLinks: false,
				}),
				starlightChangelogs(),
starlightSidebarTopics([
					{
						label: 'Getting Started',
						link: '/getting-started/installation/',
						icon: 'i-tabler:rocket',
						items: [
							{ label: 'Installation', slug: 'getting-started/installation' },
							{ label: 'Quickstart', slug: 'getting-started/quickstart' },
						],
					},
					{
						label: 'Compatibility',
						link: '/compatibility/redis-differences/',
						icon: 'i-tabler:puzzle',
						items: [
							{ label: 'Redis Differences', slug: 'compatibility/redis-differences' },
							{ label: 'Migration Guide', slug: 'compatibility/migration-guide' },
						],
					},
					{
						label: 'Guides',
						link: '/guides/event-sourcing/',
						icon: 'i-tabler:book',
						items: [
							{ label: 'Event Sourcing', slug: 'guides/event-sourcing' },
						],
					},
					{
						label: 'Operations',
						link: '/operations/deployment/',
						icon: 'i-tabler:settings',
						items: [
							{ label: 'Deployment', slug: 'operations/deployment' },
							{ label: 'Persistence', slug: 'operations/persistence' },
							{ label: 'Replication', slug: 'operations/replication' },
							{ label: 'Clustering', slug: 'operations/clustering' },
							{ label: 'Monitoring', slug: 'operations/monitoring' },
							{ label: 'Observability', slug: 'operations/observability' },
							{ label: 'Debug UI & HTTP API', slug: 'operations/debug-ui' },
							{ label: 'Performance Tools', slug: 'operations/performance' },
							{ label: 'Security', slug: 'operations/security' },
							{ label: 'Backup & Restore', slug: 'operations/backup-restore' },
							{ label: 'Kubernetes', slug: 'operations/kubernetes' },
						],
					},
					{
						label: 'Reference',
						link: '/reference/commands/',
						icon: 'i-tabler:file-text',
						items: [
							{ label: 'Commands', slug: 'reference/commands' },
							{ label: 'Configuration', slug: 'reference/configuration' },
							{ label: 'Reference Config', slug: 'reference/reference-config' },
							{ label: 'frogdb-server', slug: 'reference/frogdb-server' },
							{ label: 'frogctl', slug: 'reference/frogctl' },
							{ label: 'Metrics', slug: 'reference/metrics' },
							{ label: 'Benchmarks', slug: 'reference/benchmarks' },
						],
					},
					{
						label: 'Architecture',
						link: '/architecture/architecture/',
						icon: 'i-tabler:cpu',
						items: [
							{
								label: 'Architecture',
								autogenerate: { directory: 'architecture' },
							},
						],
					},
				], {
					exclude: ['/changelog', '/changelog/**/*'],
				}),
			],
			title: 'FrogDB',
			favicon: '/favicon.png',
			logo: {
				src: './src/assets/frogdb-logo-128.png',
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
		}),
	],
});
