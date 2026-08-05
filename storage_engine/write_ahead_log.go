package main
import (
	// "bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"log"
)
var counter uint64 = 0  
type WALInput struct{
	TransactionId uint64
	Op uint8
	Key []byte
	KeyLen uint16
	Value []byte
	ValueLen uint32
	CheckSum uint32
}

func OpenOrCreateFile(file string) (*os.File, error) {
	// If the file doesn't exist, create it, or append to the file
    f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
    if err != nil {
        return nil, err
    }
    

    return f, nil 
}

func SaveCounter(id int64) error {
	f, err := os.Create("counter.bin")
	if err !=nil{
		log.Fatal("couldn't open the file")
		return err
	}
	return binary.Write(f, binary.LittleEndian, id);
	
}

func LoadCounter(file string) (int64, error) {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
    if err != nil {
        return -1, err
    }
	defer f.Close()
	TId := make([]byte, 8)
	_, err = f.Read(TId)
	if err != nil {
		fmt.Println("couldn't read the transactionId")
		return -1, err
	}
	return int64(binary.LittleEndian.Uint64(TId)), nil
}

func WriteToWAL(Op uint8, Key []byte, Value []byte) int {

	f, err := OpenOrCreateFile("WAL.bin")
	if err != nil { 
		log.Fatal(err)
		return 0 
	}
	defer f.Close()

	curr_counter, err := LoadCounter("counter.bin")
	if err != nil { 
		fmt.Println(err)
		// create the first counter in the file 
		SaveCounter(0)
	}
	curr_counter += 1
	WALRecord := WALInput { 
		TransactionId: uint64(curr_counter),
		Op: Op, // INSERT,PUT,DELETE
		Key: Key,
		KeyLen: uint16(len(Key)),
		Value: Value,
		ValueLen: uint32(len(Value)),
		CheckSum:0, // do checksum calc later
	}

	data := []byte(fmt.Sprintf("%d%d%d", WALRecord.Key, WALRecord.Value, WALRecord.TransactionId))
	WALRecord.CheckSum = crc32.ChecksumIEEE(data)
	
	// write to the file
	binary.Write(f, binary.LittleEndian, WALRecord.TransactionId)
	binary.Write(f, binary.LittleEndian, WALRecord.Op)
	binary.Write(f, binary.LittleEndian, WALRecord.KeyLen)
	binary.Write(f, binary.LittleEndian, WALRecord.Key)
	binary.Write(f, binary.LittleEndian, WALRecord.ValueLen)
	binary.Write(f, binary.LittleEndian, WALRecord.Value)
	binary.Write(f, binary.LittleEndian, WALRecord.CheckSum)
	// write to the counter.bin file 
	SaveCounter(int64(WALRecord.TransactionId))

	return 1
}

func RecoverFromWAL (file string) (int, WALInput) {
	// f, err := OpenOrCreateFile(file)
	// if err != nil { 
	// 	log.Fatal(err)
	// 	return 0 
	// }
	// defer f.Close()

	f, err := os.Open(file)
	if err != nil{
		log.Fatal(err)
	}

	defer f.Close()
	
	WALRecords := []WALInput{}

	for {
		
		//read transactionId
		TId := make([]byte, 8)
		Op := make([]byte, 1)
		
		KeyLen:= make([]byte, 2)
		ValueLen := make([]byte, 4)
		CheckSum := make([]byte, 4)

		_, err = f.Read(TId)
		if err != nil {
			break
		}

		_, err = f.Read(Op)
		if err != nil {
			break
		}

		_, err = f.Read(KeyLen)
		if err != nil {
			break
		}

		Key := make([]byte, binary.LittleEndian.Uint16(KeyLen))

		_, err = f.Read(Key)
		if err != nil {
			break
		}

		_, err = f.Read(ValueLen)
		if err != nil {
			break
		}

		Value := make([]byte, binary.LittleEndian.Uint32(ValueLen))

		_, err = f.Read(Value)
		if err != nil {
			break
		}

		_, err = f.Read(CheckSum)
		if err != nil {
			break
		}

		WALRecord := WALInput{
			TransactionId: binary.LittleEndian.Uint64(TId),
			Op: Op[0],
			Key: Key,
			KeyLen: uint16(len(Key)),
			Value: Value,
			ValueLen: uint32(len(Value)),
			CheckSum: binary.LittleEndian.Uint32(CheckSum),
		}

		WALRecords = append(WALRecords, WALRecord)

	}

	return len(WALRecords), WALRecords[len(WALRecords)-1]
}


