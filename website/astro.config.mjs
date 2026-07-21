// @ts-check
import { defineConfig } from 'astro/config';
import UnoCSS from 'unocss/astro';
import starlight from '@astrojs/starlight';
import starlightChangelogs from 'starlight-changelogs';
import starlightThemeRapide from 'starlight-theme-rapide';
import starlightLinksValidator from 'starlight-links-validator';
import starlightSidebarTopics from 'starlight-sidebar-topics';
import { starlightIconsPlugin } from 'starlight-plugin-icons';
import remarkBaseUrl from './plugins/remark-base-url.mjs';
import { frogDark, frogLight } from './src/frog-code-themes.ts';

const BASE = '/';

// https://astro.build/config
export default defineConfig({
	site: 'https://frogdb.dev',
	base: BASE,
	redirects: {
		// Cut / superseded compatibility pages.
		'/compatibility/migration-guide/': '/compatibility/overview/',
		'/compatibility/redis-differences/': '/compatibility/overview/',
		'/compatibility/redis-test-suite/': '/compatibility/test-suite/',
		// Command reference folded into the generated compatibility matrix.
		'/reference/commands/': '/compatibility/command-matrix/',
		// Testing methodology moved out of Architecture.
		'/architecture/testing/': '/compatibility/testing-methodology/',
		// Guides topic dissolved into Extensions.
		'/guides/': '/extensions/overview/',
		'/guides/event-sourcing/': '/extensions/event-sourcing/',
		// Operations merges: monitoring→observability, performance+debug-ui→diagnostics,
		// kubernetes→deployment.
		'/operations/monitoring/': '/operations/observability/',
		'/operations/performance/': '/operations/diagnostics/',
		'/operations/debug-ui/': '/operations/diagnostics/',
		'/operations/kubernetes/': '/operations/deployment/',
		// Cut / replaced reference pages.
		'/reference/reference-config/': '/reference/example-config/',
		'/reference/benchmarks/': '/reference/configuration/',
	},
	markdown: {
		remarkPlugins: [[remarkBaseUrl, { base: BASE }]],
	},
	integrations: [
		UnoCSS(),
		starlight({
			expressiveCode: {
				themes: [frogDark, frogLight],
				shiki: {
					langAlias: { redis: 'text' },
				},
			},
			plugins: [
				starlightThemeRapide(),
				starlightIconsPlugin({ sidebar: true, extractSafelist: true }),
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
						label: 'Compatibility & Correctness',
						link: '/compatibility/overview/',
						icon: 'i-tabler:plug-connected',
						items: [
							{ label: 'Overview & differences', slug: 'compatibility/overview' },
							{ label: 'Command matrix', slug: 'compatibility/command-matrix' },
							{ label: 'Test suite results', slug: 'compatibility/test-suite' },
							{ label: 'Testing methodology', slug: 'compatibility/testing-methodology' },
						],
					},
					{
						label: 'Extensions',
						link: '/extensions/overview/',
						icon: 'i-tabler:puzzle',
						items: [
							{ label: 'Overview', slug: 'extensions/overview' },
							{ label: 'Event Sourcing', slug: 'extensions/event-sourcing' },
						],
					},
					{
						label: 'Operations',
						link: '/operations/configuration/',
						icon: 'i-tabler:dashboard',
						items: [
							{ label: 'Configuration', slug: 'operations/configuration' },
							{ label: 'Deployment', slug: 'operations/deployment' },
							{ label: 'Persistence', slug: 'operations/persistence' },
							{ label: 'Replication', slug: 'operations/replication' },
							{ label: 'Clustering', slug: 'operations/clustering' },
							{ label: 'Security', slug: 'operations/security' },
							{ label: 'Observability', slug: 'operations/observability' },
							{ label: 'Diagnostics', slug: 'operations/diagnostics' },
							{ label: 'Backup & Restore', slug: 'operations/backup-restore' },
						],
					},
					{
						label: 'Reference',
						link: '/reference/configuration/',
						icon: 'i-tabler:file-text',
						items: [
							{ label: 'Configuration', slug: 'reference/configuration' },
							{ label: 'Example Config', slug: 'reference/example-config' },
							{ label: 'frogdb-server', slug: 'reference/frogdb-server' },
							{ label: 'frogctl', slug: 'reference/frogctl' },
							{ label: 'Metrics', slug: 'reference/metrics' },
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
			// Site ships a hand-tuned dark theme; tell Dark Reader to leave it alone.
			head: [{ tag: 'meta', attrs: { name: 'darkreader-lock' } }],
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
				baseUrl: 'https://github.com/frogdb/frogdb/edit/main/website/',
			},
			customCss: [
				'@fontsource-variable/inter',
				'./src/styles/custom.css',
			],
			components: {
				Hero: './src/components/Hero.astro',
			},
		}),
	],
});
