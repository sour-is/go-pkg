// SPDX-FileCopyrightText: 2023 Jon Lundy <jon@xuu.cc>
// SPDX-License-Identifier: BSD-3-Clause

package xdg

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

var (
	EnvDataHome        = setENV("XDG_DATA_HOME", defaultDataHome)
	EnvDataDirs        = setENV("XDG_DATA_DIRS", defaultDataDirs)
	EnvConfigHome      = setENV("XDG_CONFIG_HOME", defaultConfigHome)
	EnvConfigDirs      = setENV("XDG_CONFIG_DIRS", defaultConfigDirs)
	EnvCacheHome       = setENV("XDG_CACHE_HOME", defaultCacheHome)
	EnvStateHome       = setENV("XDG_STATE_HOME", defaultStateHome)
	EnvRuntime         = setENV("XDG_RUNTIME_DIR", defaultRuntime)
	EnvDesktopDir      = setENV("XDG_DESKTOP_DIR", defaultDesktop)
	EnvDownloadDir     = setENV("XDG_DOWNLOAD_DIR", defaultDownload)
	EnvDocumentsDir    = setENV("XDG_DOCUMENTS_DIR", defaultDocuments)
	EnvMusicDir        = setENV("XDG_MUSIC_DIR", defaultMusic)
	EnvPicturesDir     = setENV("XDG_PICTURES_DIR", defaultPictures)
	EnvVideosDir       = setENV("XDG_VIDEOS_DIR", defaultVideos)
	EnvTemplatesDir    = setENV("XDG_TEMPLATES_DIR", defaultTemplates)
	EnvPublicShareDir  = setENV("XDG_PUBLICSHARE_DIR", defaultPublic)
	EnvApplicationsDir = setENV("XDG_APPLICATIONS_DIR", defaultApplicationDirs)
	EnvFontsDir        = setENV("XDG_FONTS_DIR", defaultFontDirs)
)

func setENV(name, value string) string {
	if _, ok := os.LookupEnv(name); !ok {
		os.Setenv(name, value)
	}
	return literal(name)
}
func Get(base, suffix string) string {
	return strings.Join(paths(base, suffix), string(os.PathListSeparator))
}
func paths(base, suffix string) []string {
	paths := strings.Split(os.ExpandEnv(base), string(os.PathListSeparator))
	for i, path := range paths {
		if strings.HasPrefix(path, "~") {
			path = strings.Replace(path, "~", getHome(), 1)
		}
		paths[i] = os.ExpandEnv(filepath.Join(path, suffix))
	}
	return paths
}
func Find(base, filename string) []string {
	var files []string
	for _, f := range paths(base, filename) {
		if ok, _ := exists(f); !ok {
			continue
		}
		files = append(files, f)
	}
	return files
}

func getHome() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return home
}

func exists(name string) (bool, error) {
	s, err := os.Stat(name)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if s.IsDir() {
		return false, nil
	}
	return false, err
}
