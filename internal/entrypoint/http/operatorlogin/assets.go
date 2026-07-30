// Package operatorlogin exposes designer-owned login assets to the HTTP
// composition root. The backend owns only the asset wiring; template and CSS
// content remain in their package-local files.
package operatorlogin

import "embed"

// Assets contains the login template and its static stylesheet.
//
//go:embed templates/login.html static/style.css
var Assets embed.FS
