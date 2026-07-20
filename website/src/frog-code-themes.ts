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
			settings: { foreground: '#a8d92e' },
		},
		{
			scope: ['keyword', 'storage.type', 'storage.modifier', 'keyword.operator'],
			settings: { foreground: '#8b7cf6' },
		},
		{
			scope: ['entity.name.function', 'support.function', 'entity.name.tag'],
			settings: { foreground: '#c3f53c' },
		},
		{
			scope: ['constant.numeric', 'constant.language', 'constant.character'],
			settings: { foreground: '#e0a43b' },
		},
		{
			scope: ['punctuation', 'meta.brace', 'punctuation.separator', 'punctuation.terminator'],
			settings: { foreground: '#8a8a8a' },
		},
		{
			scope: ['variable', 'variable.parameter', 'variable.other', 'support.variable'],
			settings: { foreground: '#d8d6d3' },
		},
		{
			scope: ['entity.name.type', 'support.type', 'support.class', 'entity.name.class'],
			settings: { foreground: '#7cc7e8' },
		},
		{
			scope: ['entity.other.attribute-name'],
			settings: { foreground: '#e0a43b' },
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
			settings: { foreground: '#55761a' },
		},
		{
			scope: ['keyword', 'storage.type', 'storage.modifier', 'keyword.operator'],
			settings: { foreground: '#6b4fc9' },
		},
		{
			scope: ['entity.name.function', 'support.function', 'entity.name.tag'],
			settings: { foreground: '#3f5a11' },
		},
		{
			scope: ['constant.numeric', 'constant.language', 'constant.character'],
			settings: { foreground: '#b07a1a' },
		},
		{
			scope: ['punctuation', 'meta.brace', 'punctuation.separator', 'punctuation.terminator'],
			settings: { foreground: '#6f6f68' },
		},
		{
			scope: ['variable', 'variable.parameter', 'variable.other', 'support.variable'],
			settings: { foreground: '#2e2e2a' },
		},
		{
			scope: ['entity.name.type', 'support.type', 'support.class', 'entity.name.class'],
			settings: { foreground: '#2f6a8a' },
		},
		{
			scope: ['entity.other.attribute-name'],
			settings: { foreground: '#b07a1a' },
		},
	],
};

export { frogDark, frogLight };
