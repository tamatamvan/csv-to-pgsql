package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

var db *pgx.Conn

func getCSVsFromDir(dir string) []fs.DirEntry {
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	return files
}

func inferDataType(data string) string {
	if _, err := strconv.ParseInt(data, 10, 64); err == nil {
		return "int"
	}
	if _, err := strconv.ParseFloat(data, 64); err == nil {
		return "decimal(10,2)"
	}
	var arr []string
	if err := json.Unmarshal([]byte(data), &arr); err == nil {
		return "text[]"
	}
	// If it's not any of the above types, return string
	return "varchar(255)"
}

func inferColumnTypes(columns []string, data []string) []string {
	var colTypes []string
	for i := 0; i < len(data); i++ {
		var inferredType string
		if columns[i] == "password" {
			inferredType = "varchar(255)"
		} else {
			inferredType = inferDataType(data[i])
		}
		colTypes = append(colTypes, columns[i]+" "+inferredType)
	}
	return colTypes
}

func createTable(csvContent CSVContent) {
	// check if table if not exists
	// Convert the columns slice to a comma-separated string
	cols := strings.Join(inferColumnTypes(csvContent.Columns, csvContent.Data[0]), ", ")

	// Prepare the SQL statement
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s);`, csvContent.TableName, cols)

	fmt.Println(sqlStatement)
	// Execute the SQL statement
	_, err := db.Exec(context.Background(), sqlStatement)
	if err != nil {
		log.Fatal(err)
		os.Exit(2)
	}

}

type CSVContent struct {
	TableName string
	Columns   []string
	Data      [][]string
}

func readCSVinDir(dir string) []CSVContent {
	csvFiles := getCSVsFromDir(dir)
	var csvContents []CSVContent

	for _, file := range csvFiles {
		tableName := strings.TrimSuffix(file.Name(), ".csv")

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

		csvContent := CSVContent{
			TableName: tableName,
			Columns:   columns,
			Data:      data,
		}

		csvContents = append(csvContents, csvContent)
	}

	return csvContents
}

func init() {
	godotenv.Load(".env")
	fmt.Println("Reading .env file")

	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	db = conn
	// defer db.Close(context.Background())

}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please provide a directory")
		os.Exit(1)
	}
	dir := os.Args[1]

	csvContents := readCSVinDir(dir)

	for _, csvContent := range csvContents {
		fmt.Println("Table Name: ", csvContent.TableName)
		createTable(csvContent)
	}
}
