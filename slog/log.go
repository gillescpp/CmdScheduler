package slog

import (
	"fmt"
	"io"
	"log"
	"os"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

//globale
var applog *log.Logger

// InitLogs initialise les logs
func InitLogs(filename string, maxSize int, maxBackup int, compression bool) {
	//instance writer à appliquer
	if filename == "" {
		//stdout
		applog = log.New(os.Stdout, "", log.LstdFlags)
	} else {
		//fichier
		w := &lumberjack.Logger{
			Filename:   filename,
			MaxSize:    maxSize,
			MaxBackups: maxBackup,
			Compress:   compression,
			LocalTime:  true,
		}
		w2 := io.MultiWriter(os.Stdout, w)
		applog = log.New(w2, "", log.LstdFlags)
	}
}

//trace log type simple trace
func Trace(section, msg string, args ...interface{}) {
	applog.Output(2, "INFO ["+section+"] "+fmt.Sprintf(msg, args...))
}

//trace log type warning
func Warning(section, msg string, args ...interface{}) {
	applog.Output(2, "WARN ["+section+"] "+fmt.Sprintf(msg, args...))
}

//trace log type error
func Error(section, msg string, args ...interface{}) {
	applog.Output(2, "ERROR ["+section+"] "+fmt.Sprintf(msg, args...))
}

//trace log type erreur fatale
func Fatal(section, msg string, args ...interface{}) {
	applog.Output(2, "FATAL ["+section+"] "+fmt.Sprintf(msg, args...))
	os.Exit(1)
}
