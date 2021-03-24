package main

import (
	"CmdScheduler/ctrl"
	"CmdScheduler/dal"
	"CmdScheduler/schd"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/securecookie"
	"github.com/spf13/viper"
)

func main() {

	insClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	//resp, clientErr := insClient.Get("https://localhost:8800/task/ping")
	resp, clientErr := insClient.Get("https://www.google.fr")
	if clientErr != nil {
		panic(clientErr)
	}
	if resp.TLS != nil {
		certificates := resp.TLS.PeerCertificates
		if len(certificates) > 0 {
			// you probably want certificates[0]
			cert := certificates[0]
			certSign := hex.EncodeToString(cert.Signature)
			println("Subject", cert.Subject.String())
			println("Issuer", cert.Issuer.String())

			publicKeyDer, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
			if err != nil {
				panic(err)
			}
			certPublicKey := string(pem.EncodeToMemory(&pem.Block{
				Type:  "PUBLIC KEY",
				Bytes: publicKeyDer,
			}))

			println("sign", certSign)
			println("pk", certPublicKey)

			//unmarshal pub
			cert1, perr := x509.ParseCertificate(cert.Raw)
			if perr != nil {
				panic(err)
			}
			certPool := x509.NewCertPool()
			certPool.AddCert(cert1)

			insClient2 := &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						RootCAs: certPool,
					},
				},
			}

			_, clientErr2 := insClient2.Get("https://localhost:8800/task/ping")
			if clientErr2 != nil {
				panic(clientErr2)
			}
		}
	}
	return

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

	//lancement task scheduleur
	schd.Start()
	defer schd.Stop()

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
