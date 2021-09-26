package main

import (
	"CmdScheduler/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

//Chargement <app name>.toml ou config.toml
func readConfig() error {
	var err error

	//def de quelques valeurs par defaut
	viper.SetDefault("http_port", 8100)
	viper.SetDefault("Title", "Cmd Scheduler")
	viper.SetDefault("db_driver", "sqlite3")
	viper.SetDefault("db_datasource", "file:data.db")
	viper.SetDefault("db_schema", "SCHED")

	//on s'appui sur viper :
	//nom du fichier de config = fourni en param
	cfgPath := ""
	cfgPath2 := ""
	if len(os.Args) > 1 && strings.TrimSpace(os.Args[1]) != "" {
		cfgPath = strings.TrimSpace(os.Args[1])
	} else {
		//on recherche relativement à l'exe <appname>.toml ou config.toml
		execPath, err := os.Executable()
		if err != nil {
			return err
		}

		appNameWithExt := filepath.Base(execPath)
		appName := strings.TrimSuffix(appNameWithExt, filepath.Ext(appNameWithExt))
		exeDir := filepath.Dir(execPath)
		cfgPath = filepath.Join(exeDir, appName+".toml")
		cfgPath2 = filepath.Join(exeDir, "config.toml") //a defaut de cfgPath
	}

	//lecture fichier dispo...
	cfgPathToUse := ""
	for i := 0; i < 2; i++ {
		if i == 0 {
			cfgPathToUse = cfgPath
		} else {
			cfgPathToUse = cfgPath2
		}
		if cfgPathToUse != "" {
			_, err = os.Stat(cfgPathToUse)
			if err != nil {
				cfgPathToUse = ""
			} else {
				break
			}
		}
	}
	if err != nil {
		return err //1 fichier requis
	}

	slog.Trace("main", "Read %v", cfgPathToUse)
	viper.SetConfigFile(cfgPathToUse)
	err = viper.ReadInConfig()
	if err != nil {
		return err
	}

	return nil
}
