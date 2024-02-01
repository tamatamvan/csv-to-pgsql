package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io/fs"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/jackc/pgx/v5"
)

func getCSVsFromDir(dir string) []fs.DirEntry {
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	return files
}

func readCSVinDir(dir string) {
	csvFiles := getCSVsFromDir(dir)

	for _, file := range csvFiles {
		tableName := strings.TrimSuffix(file.Name(), ".csv")
		fmt.Println("Table Name: ", tableName)

		contents, err := os.ReadFile(dir + "/" + file.Name())

		if err != nil {
			log.Fatal(err)
		}
		r := csv.NewReader(strings.NewReader(string(contents)))
		records, err1 := r.ReadAll()
		if err1 != nil {
			log.Fatal(err)
		}
		columns := records[0]
		data := records[1:]
		fmt.Println("Columns: ", columns)
		fmt.Println("Data: ", data)
	}

}

func init() {
	godotenv.Load(".env")
	fmt.Println("Reading .env file")

	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	fmt.Println(greeting)

}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please provide a directory")
		os.Exit(1)
	}
	dir := os.Args[1]
	readCSVinDir(dir)
}
