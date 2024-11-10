package pkg

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/sujanks/data-gen-app/pkg/sink"
	"gopkg.in/inf.v0"
	"gopkg.in/yaml.v3"
)

type Tables struct {
	Tables []Table `yaml:"tables`
}

type Table struct {
	Name     string   `yaml:"name"`
	Priority int      `yaml:"priority"`
	Columns  []Column `yaml:"columns"`
}

type Column struct {
	Name      string   `yaml:"name"`
	Pattern   string   `yaml:"pattern"`
	Value     []string `yaml:"value"`
	Type      string   `yaml:"type"`
	Mandatory string   `yaml:"madatory"`
	Parent    bool     `yaml:"parent`
	Foreign   string   `yaml:"foreign"`
}

const hashtag = '#'

func GenerateData(ds sink.DataSink, count int, profile string) {
	tables := readManifest(profile)

	parentKeyValues := make(map[string][]string, 0)

	for _, table := range tables.Tables {
		for i := 0; i < count; i++ {
			var tableData = make(map[string]interface{})
			for _, col := range table.Columns {
				var colValue interface{}
				if col.Mandatory == "" {
					if col.Foreign != "" {
						colValue = gofakeit.RandomString(parentKeyValues[col.Foreign])
					} else if len(col.Value) > 0 {
						colValue = gofakeit.RandomString(col.Value)
					} else if col.Pattern != "" {
						colValue = replaceWithNumbers(col.Pattern)
					} else {
						switch col.Type {
						case "float":
							colValue = inf.NewDec(1, 0)
						case "sentence":
							colValue = gofakeit.Sentence(5)
						case "bool":
							colValue = gofakeit.Bool()
						case "int":
							colValue = gofakeit.Int()
						case "date":
							colValue = time.Now().Format("2006-01-02")
						case "timestamp":
							colValue = time.Now()
						case "uuid":
							colValue = gofakeit.UUID()
						default:
							if strings.Contains(col.Name, "name") {
								colValue = gofakeit.Name()
							} else {
								colValue = gofakeit.Word()
							}
						}
					}
				}
				tableData[col.Name] = colValue

				temp := fmt.Sprint(colValue)
				if col.Parent {
					keyName := fmt.Sprintf("%s:%s", table.Name, col.Name)
					valuesArray := parentKeyValues[keyName]
					valuesArray = append(valuesArray, temp)
					parentKeyValues[keyName] = valuesArray
				}
			}
			ds.InsertRcord(table.Name, tableData)
		}
	}
	log.Printf("%d records inserted", count)
}

func readManifest(filename string) Tables {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("error reading file %v ", err.Error())
	}

	var tables Tables
	err = yaml.NewDecoder(file).Decode(&tables)
	if err != nil {
		log.Fatalf("error reading file %v ", err.Error())
	}
	sort.Sort(ByTablePriority(tables.Tables))
	return tables

}

func replaceWithNumbers(str string) string {
	if str == "" {
		return str
	}
	bytestr := []byte(str)
	for i := 0; i < len(bytestr); i++ {
		if bytestr[i] == hashtag {
			bytestr[i] = byte(randDigit())
		}
	}
	if bytestr[0] == '0' {
		bytestr[0] = byte(gofakeit.IntN(8)+1) + '0'
	}
	return string(bytestr)
}

func randDigit() rune {
	return rune(byte(gofakeit.IntN(10)) + '0')
}

type ByTablePriority []Table

func (m ByTablePriority) Len() int           { return len(m) }
func (m ByTablePriority) Less(i, j int) bool { return m[i].Priority > m[j].Priority }
func (m ByTablePriority) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
