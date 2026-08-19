// Package web embeds the built React SPA so the single binary serves the
// dashboard and the API from one container. Build the frontend with
// `npm run build` (in web/) before building the Go binary; the output must
// exist in dist/ for this package to compile.
package web

import "embed"

//go:embed dist
var dist embed.FS

// FS returns the embedded static assets for the React dashboard.
func FS() embed.FS {
	return dist
}
