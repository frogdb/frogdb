// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'FrogDB',
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
					label: 'Guides',
					items: [
						{ label: 'Persistence', slug: 'guides/persistence' },
						{ label: 'Replication', slug: 'guides/replication' },
						{ label: 'Clustering', slug: 'guides/clustering' },
						{ label: 'Lua Scripting', slug: 'guides/lua-scripting' },
					],
				},
				{
					label: 'Operations',
					items: [
						{ label: 'Deployment', slug: 'operations/deployment' },
						{ label: 'Monitoring', slug: 'operations/monitoring' },
						{ label: 'Backup & Restore', slug: 'operations/backup-restore' },
						{ label: 'Troubleshooting', slug: 'operations/troubleshooting' },
					],
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
