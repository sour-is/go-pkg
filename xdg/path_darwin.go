//go:build darwin
// +build darwin
// SPDX-FileCopyrightText: 2023 Jon Lundy <jon@xuu.cc>
// SPDX-License-Identifier: BSD-3-Clause

package xdg

func literal(name string) string {
	return "$" + name
}

const (
	defaultDataHome   = "~/Library/Application Support"
	defaultDataDirs   = "/Library/Application Support"
	defaultConfigHome = "~/Library/Preferences"
	defaultConfigDirs = "/Library/Preferences"
	defaultCacheHome  = "~/Library/Caches"
	defaultStateHome  = "~/Library/Caches"
	defaultRuntime    = "~/Library/Application Support"

	defaultDesktop   = "~/Desktop"
	defaultDownload  = "~/Downloads"
	defaultDocuments = "~/Documents"
	defaultMusic     = "~/Music"
	defaultPictures  = "~/Pictures"
	defaultVideos    = "~/Videos"
	defaultTemplates = "~/Templates"
	defaultPublic    = "~/Public"

	defaultApplicationDirs = "~/Applications:/Applications"
	defaultFontDirs        = "~/Library/Fonts:/Library/Fonts:/System/Library/Fonts:/Network/Library/Fonts"
)
