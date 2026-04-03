import { defineConfig } from 'unocss'
import { presetStarlightIcons } from 'starlight-plugin-icons/uno'

export default defineConfig({
	presets: [presetStarlightIcons()],
	safelist: [
		'i-tabler:rocket',
		'i-tabler:plug-connected',
		'i-tabler:book',
		'i-tabler:dashboard',
		'i-tabler:file-text',
		'i-tabler:cpu',
	],
})
