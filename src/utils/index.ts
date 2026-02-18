export function createPageUrl(pageName: string) {
    const [withoutHash, hashFragment] = String(pageName).split('#', 2);
    const [rawPath, queryString] = withoutHash.split('?', 2);
    const normalizedPath = rawPath
        .replace(/^\/+/, '')
        .replace(/ /g, '-')
        .toLowerCase();

    let url = `/${normalizedPath}`;
    if (queryString !== undefined) url += `?${queryString}`;
    if (hashFragment !== undefined) url += `#${hashFragment}`;
    return url;
}
