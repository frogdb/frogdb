import { visit } from 'unist-util-visit';

const PROTOCOL_RE = /^[a-z][a-z0-9+.-]*:/i;

function prependBase(url, base) {
  if (!url || !url.startsWith('/') || url.startsWith('//')) return url;
  if (PROTOCOL_RE.test(url)) return url;
  if (url === base || url.startsWith(base + '/') || url.startsWith(base + '#')) return url;

  // Handle hash fragments: /path/#section → /frogdb/path/#section
  const hashIndex = url.indexOf('#');
  if (hashIndex > 0) {
    return base + url.slice(0, hashIndex) + url.slice(hashIndex);
  }

  return base + url;
}

export default function remarkBaseUrl({ base } = {}) {
  if (!base || base === '/') return () => {};

  return (tree) => {
    visit(tree, (node) => {
      // Markdown links and reference definitions
      if ((node.type === 'link' || node.type === 'definition') && node.url) {
        node.url = prependBase(node.url, base);
      }

      // MDX JSX components (e.g. <LinkCard href="/path/" />)
      if (
        (node.type === 'mdxJsxFlowElement' || node.type === 'mdxJsxTextElement') &&
        node.attributes
      ) {
        for (const attr of node.attributes) {
          if (attr.name === 'href' && typeof attr.value === 'string') {
            attr.value = prependBase(attr.value, base);
          }
        }
      }
    });
  };
}
