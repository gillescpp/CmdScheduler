package main

import (
	"CmdScheduler/ctrl"
	"CmdScheduler/dal"
	"encoding/hex"
	"log"
	"strconv"

	"github.com/gorilla/securecookie"
	"github.com/spf13/viper"
)

func main() {
	//lecture config
	err := readConfig()
	if err != nil {
		log.Fatalln("readConfig", err)
	}

	//prepa db
	err = dal.InitDb(viper.GetString("db_driver"), viper.GetString("db_datasource"), viper.GetString("db_schema"))
	if err != nil {
		log.Fatalln("InitDb", err)
	}

	//init session key
	err = initSessionKey()
	if err != nil {
		log.Fatalln("initSessionKey", err)
	}

	//Mise en écoute de l'interface REST
	restPort := viper.GetInt("http_port")
	strListenOn := ":" + strconv.Itoa(restPort)
	log.Println("Listening on", strListenOn, "...")
	log.Fatal(ctrl.ListenAndServe(strListenOn))
}

// initSessionKey SESSION_KEY
func initSessionKey() error {
	sk, err := dal.CfgKVGet("web.session_key")
	if err != nil {
		return err
	}
	if len(sk) < 64 {
		bk := securecookie.GenerateRandomKey(32)[:]
		sk = hex.EncodeToString(bk) //stockée en hex pour stocker en simple lisible sur la bdd

		err = dal.CfgKVSet("web.session_key", sk)
		if err != nil {
			return err
		}
	}
	v, err := hex.DecodeString(sk)
	if err != nil {
		return err
	}
	ctrl.SessionKey = v

	return nil
}
