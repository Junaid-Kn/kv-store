package main

import ("fmt")

func main() {
	key := []byte("key2")
	val := []byte("1230490223")
    var w int = WriteToWAL(1, key, val)
	length, recovered := RecoverFromWAL("WAL.bin")
	fmt.Println(w)
	fmt.Println(length)
	fmt.Println(recovered)
	fmt.Println(string(recovered.Key))
	fmt.Println(LoadCounter("counter.bin"))

}
