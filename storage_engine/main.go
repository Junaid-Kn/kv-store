package main

import ("fmt")

func main() {

	s := new(KVStorage)

	key := []byte("key2")
	val := []byte("1230490223")
    err := s.WriteToWAL(1, key, val)
	if err != nil{
		return
	}
	length, recovered := RecoverFromWAL("WAL.bin")
	// fmt.Println(w)
	fmt.Println(length)
	fmt.Println(recovered)
	// fmt.Println(string(recovered.Key))
	// fmt.Println(LoadCounter("counter.bin"))

}
