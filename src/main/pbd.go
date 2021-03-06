package main

//
// see directions in pbc.go
//

import "time"
import "github.com/l1nkkk/6.824/src/pbservice"
import "os"
import "fmt"

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: pbd viewport myport\n")
		os.Exit(1)
	}

	pbservice.StartServer(os.Args[1], os.Args[2])

	for {
		time.Sleep(100 * time.Second)
	}
}
