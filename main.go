package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func getCSVsFromDir(dir string) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
			log.Fatal(err)
	}

	for _, file := range files {
			if filepath.Ext(file.Name()) == ".csv" {
					fmt.Println(file.Name())
			}
	}
}

func main() {
	if len(os.Args) < 2 {
			fmt.Println("Please provide a directory")
			os.Exit(1)
	}
	dir := os.Args[1]
	getCSVsFromDir(dir)
}