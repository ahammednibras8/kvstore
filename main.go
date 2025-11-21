package main

import "fmt"

func main() {
	alpha := 0.4
	var temp float64 = 0

	for t := 0; t <= 30; t++ {
		var load float64
		switch {
		case t < 10:
			load = 0
		case t < 20:
			load = 1000
		default:
			load = 0
		}

		temp = alpha*load + (1-alpha)*temp

		fmt.Printf("t=%02d load=%4.0f temp=%.2f\n", t, load, temp)
	}
}
