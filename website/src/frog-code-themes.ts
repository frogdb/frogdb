/**
 * Custom Shiki theme pair matching the FrogDB "data report" palette used in
 * `src/styles/custom.css`. Passed to `expressiveCode.themes` in
 * `astro.config.mjs`, which starlight-theme-rapide's `config:setup` hook
 * spreads over its own `['vitesse-dark', 'vitesse-light']` default — see
 * `node_modules/starlight-theme-rapide/index.ts`. Because our
 * `expressiveCode` object is spread *after* rapide's defaults, our `themes`
 * key wins outright; no patching of rapide is required.
 */
import type { ThemeInput } from '@shikijs/types';

const frogDark: ThemeInput = {
	name: 'frog-dark',
	type: 'dark',
	colors: {
		'editor.background': '#101010',
		'editor.foreground': '#e8e6e3',
	},
	tokenColors: [
		{
			settings: { foreground: '#e8e6e3' },
		},
		{
			scope: ['comment'],
			settings: { foreground: '#585856', fontStyle: 'italic' },
		},
		{
			scope: ['string', 'string.regexp'],
			settings: { foreground: '#a8d92e' }, // lime
		},
		{
			scope: ['keyword', 'storage.type', 'storage.modifier', 'keyword.operator'],
			settings: { foreground: '#8b87fa' }, // violet
		},
		{
			scope: ['entity.name.function', 'support.function', 'entity.name.tag'],
			settings: { foreground: '#e386f0' }, // pink
		},
		{
			scope: ['constant.numeric', 'constant.language', 'constant.character'],
			settings: { foreground: '#e0a43b' }, // amber
		},
		{
			scope: ['punctuation', 'meta.brace', 'punctuation.separator', 'punctuation.terminator'],
			settings: { foreground: '#8a8a8a' }, // gray
		},
		{
			scope: ['variable', 'variable.parameter', 'variable.other', 'support.variable'],
			settings: { foreground: '#d8d6d3' }, // gray-1
		},
		{
			scope: ['entity.name.type', 'support.type', 'support.class', 'entity.name.class'],
			settings: { foreground: '#94d2e3' }, // sky
		},
		{
			scope: ['entity.other.attribute-name'],
			settings: { foreground: '#ffaaa1' }, // coral
		},
		// --- Shell: keep most tokens at the default foreground so bash reads
		// calmly. Only quoted strings, expansions/subshells, and escapes (below,
		// via the global rules) stay colored. Scopes are qualified with
		// `source.shell` so other languages keep their full highlighting. ---
		{
			scope: [
				'source.shell entity.name.function',
				'source.shell entity.name.command',
				'source.shell support.function',
				'source.shell string.unquoted.argument',
				'source.shell constant.numeric',
				'source.shell constant.other.option',
				'source.shell storage.modifier',
				'source.shell keyword.operator.assignment',
				'source.shell variable.other.assignment',
			],
			settings: { foreground: '#e8e6e3' }, // default fg
		},
		{
			scope: [
				'source.shell variable.other.normal',
				'source.shell punctuation.definition.variable',
				'source.shell punctuation.definition.subshell',
				'source.shell keyword.operator.expansion',
			],
			settings: { foreground: '#e0a43b' }, // amber — $vars / ${…} / $(…)
		},
	],
};

const frogLight: ThemeInput = {
	name: 'frog-light',
	type: 'light',
	colors: {
		'editor.background': '#f6f5f1',
		'editor.foreground': '#1a1a17',
	},
	tokenColors: [
		{
			settings: { foreground: '#1a1a17' },
		},
		{
			scope: ['comment'],
			settings: { foreground: '#9c9c94', fontStyle: 'italic' },
		},
		{
			scope: ['string', 'string.regexp'],
			settings: { foreground: '#55761a' }, // lime
		},
		{
			scope: ['keyword', 'storage.type', 'storage.modifier', 'keyword.operator'],
			settings: { foreground: '#5a4fcf' }, // violet
		},
		{
			scope: ['entity.name.function', 'support.function', 'entity.name.tag'],
			settings: { foreground: '#a5359a' }, // pink
		},
		{
			scope: ['constant.numeric', 'constant.language', 'constant.character'],
			settings: { foreground: '#b07a1a' }, // amber
		},
		{
			scope: ['punctuation', 'meta.brace', 'punctuation.separator', 'punctuation.terminator'],
			settings: { foreground: '#6f6f68' }, // gray
		},
		{
			scope: ['variable', 'variable.parameter', 'variable.other', 'support.variable'],
			settings: { foreground: '#2e2e2a' }, // gray-1
		},
		{
			scope: ['entity.name.type', 'support.type', 'support.class', 'entity.name.class'],
			settings: { foreground: '#2f6a8a' }, // sky
		},
		{
			scope: ['entity.other.attribute-name'],
			settings: { foreground: '#c1584a' }, // coral
		},
		// Shell: mirror the dark theme — mostly foreground, colored exceptions.
		{
			scope: [
				'source.shell entity.name.function',
				'source.shell entity.name.command',
				'source.shell support.function',
				'source.shell string.unquoted.argument',
				'source.shell constant.numeric',
				'source.shell constant.other.option',
				'source.shell storage.modifier',
				'source.shell keyword.operator.assignment',
				'source.shell variable.other.assignment',
			],
			settings: { foreground: '#1a1a17' }, // default fg
		},
		{
			scope: [
				'source.shell variable.other.normal',
				'source.shell punctuation.definition.variable',
				'source.shell punctuation.definition.subshell',
				'source.shell keyword.operator.expansion',
			],
			settings: { foreground: '#b07a1a' }, // amber — $vars / ${…} / $(…)
		},
	],
};

export { frogDark, frogLight };
