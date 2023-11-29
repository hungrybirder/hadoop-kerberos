package main

import (
	"fmt"
	"log"
	"os"

	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	krb "github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
)

func getKerberosClientWithKeytab(username, realm string, krb5ConfigPath, keytabPath string) *krb.Client {

	cfg, err := config.Load(krb5ConfigPath)
	if err != nil {
		log.Println("Couldn't load krb config:", err)
		return nil
	}
	kt, err := keytab.Load(keytabPath)
	if err != nil {
		log.Println("Couldn't load keytab:", err)
		return nil
	}

	client := krb.NewWithKeytab(username, realm, kt, cfg)
	err = client.Login()
	if err != nil {
		log.Println("Kerberos login failed: ", err)
		return nil
	} else {
		log.Println("Kerberos login success ")
	}
	return client
}

func NewHDFSClient(username, realm string, krb5ConfigPath, krb5CCName string, nodenames []string) *hdfs.Client {

	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		fmt.Println("Problem loading configuration: ", err)
	}
	options := hdfs.ClientOptionsFromConf(conf)

	options.Addresses = nodenames
	options.KerberosClient = getKerberosClientWithKeytab(username, realm, krb5ConfigPath, krb5CCName)
	options.KerberosServicePrincipleName = username
	options.User = "hdfs"

	log.Println("options.Addresses:", options.Addresses)
	log.Println("options.KerberosClient:", options.KerberosClient)
	log.Println("options.KerberosServicePrincipleName:", options.KerberosServicePrincipleName)
	log.Println("options.User:", options.User)
	client, err := hdfs.NewClient(options)

	if err != nil {
		log.Panicln("client error:", err)
	}

	return client
}

func main() {

	address := make([]string, 10)
	// cat /etc/hosts
	// 127.0.0.1 kerberos.example.com
	address = append(address, "127.0.0.1:9000")
	user := "hdfs/nn.example.com"
	client := NewHDFSClient(user, "EXAMPLE.COM", "krb5.conf", "hdfs.keytab", address)

	//fmt.Println(client.ListXAttrs("/home/"))

	path := "/user/ifilonenko/testdir"
	err := client.MkdirAll(path, 0777) // 创建testdir目录
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	//fmt.Printf("Created directory: %s\n", path)

}
