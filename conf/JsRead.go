package conf

import (
	"io/ioutil"
	"log"
)

func jsFileReader(f string) (contain string) {
	dat, err := ioutil.ReadFile(f)
	if err != nil {
		log.Printf("%v", err.Error())
	}
	return string(dat)
}
