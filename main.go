package main

import (
	"fmt"
	"os"

	periodic "github.com/zssvaidar/go-labs/labs/1-periodic"
)

var labs = map[string]func(){
	"1": periodic.Run,
}

func main() {
	lab := "1"
	if len(os.Args) > 1 {
		lab = os.Args[1]
	}
	run, ok := labs[lab]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown lab %q\n", lab)
		os.Exit(1)
	}
	run()
}
