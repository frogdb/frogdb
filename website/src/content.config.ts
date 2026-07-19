import { defineCollection, z } from 'astro:content';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';
import { changelogsLoader } from 'starlight-changelogs/loader';

export const collections = {
	docs: defineCollection({
		loader: docsLoader(),
		schema: docsSchema({
			extend: z.object({
				/** Small uppercase label rendered above the hero headline. */
				eyebrow: z.string().optional(),
				/** Outlined pill tags rendered below the hero lede. */
				pills: z.array(z.string()).optional(),
			}),
		}),
	}),
	changelogs: defineCollection({
		loader: changelogsLoader([
			{
				provider: 'keep-a-changelog',
				base: 'changelog',
				changelog: '../CHANGELOG.md',
				title: 'Changelog',
			},
		]),
	}),
};
