package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/sujanks/data-gen-app/pkg"
	"github.com/sujanks/data-gen-app/pkg/sink"
)

func main() {
	profile := os.Getenv("PROFILE")
	records := os.Getenv("RECORDS")
	count, _ := strconv.Atoi(records)
	sink := getDataSink(profile)
	manifestPath := fmt.Sprintf("./manifest/%s.yaml", profile)
	pkg.GenerateData(sink, count, manifestPath)
}

func getDataSink(profile string) sink.DataSink {
	dataSink := os.Getenv("SINK")
	switch dataSink {
	case "pg":
		return sink.NewPgDataSink(profile)
	default:
		log.Fatal("no data sink specified")
	}
	return nil
}
