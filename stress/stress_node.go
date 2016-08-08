package main

import (
	"flag"
	"fmt"
	"gfs_stress"
	"os"
)

func main() {
	gfs_stress.WritePID()
	id := flag.String("id", "", "server ID")
	role := flag.String("role", "", "master/chunkserver")
	listen := flag.String("listen", "", "listen address")
	master := flag.String("master", "", "master address")
	center := flag.String("center", "", "stress test center address")
	flag.Parse()

	if *id == "" ||
		*role == "" ||
		*center == "" ||
		*listen == "" ||
		(*role != "master" && *role != "chunkserver") ||
		(*role == "master" && *master != "") ||
		(*role == "chunkserver" && *master == "") {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	gfs_stress.Run(gfs_stress.Config{
		ID:     *id,
		Role:   *role,
		Listen: *listen,
		Master: *master,
		Center: *center,
	})
}
