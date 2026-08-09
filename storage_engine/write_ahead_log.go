package storage_engine
import (
	// "bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"log"
	"io"
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

func WriteToWAL(Key []byte, Value []byte) error {

	f, err := OpenOrCreateFile("WAL.bin")
	if err != nil { 
		return err
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
		Op: 1, // INSERT,PUT,DELETE
		Key: Key,
		KeyLen: uint16(len(Key)),
		Value: Value,
		ValueLen: uint32(len(Value)),
		CheckSum:0, 
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

	return nil
}


func RecoverFromWAL(file string) (int, WALInput) {
    f, err := os.Open(file)
    if err != nil {
        log.Println("failed to open WAL:", err)
        return 0, WALInput{}
    }
    defer f.Close()

    WALRecords := []WALInput{}

    for {
        var transactionID uint64
        var op uint8
        var keyLen uint16
        var valueLen uint32
        var checksum uint32

        // Transaction ID
        err = binary.Read(f, binary.LittleEndian, &transactionID)
        if err != nil {
            break
        }

        // Operation
        err = binary.Read(f, binary.LittleEndian, &op)
        if err != nil {
            break
        }

        // Key length
        err = binary.Read(f, binary.LittleEndian, &keyLen)
        if err != nil {
            break
        }

        // Key
        key := make([]byte, keyLen)
        _, err = io.ReadFull(f, key)
        if err != nil {
            break
        }

        // Value length
        err = binary.Read(f, binary.LittleEndian, &valueLen)
        if err != nil {
            break
        }

        // Value
        value := make([]byte, valueLen)
        _, err = io.ReadFull(f, value)
        if err != nil {
            break
        }

        // Checksum
        err = binary.Read(f, binary.LittleEndian, &checksum)
        if err != nil {
            break
        }

        WALRecord := WALInput{
            TransactionId: transactionID,
            Op:            op,
            Key:           key,
            KeyLen:        keyLen,
            Value:         value,
            ValueLen:      valueLen,
            CheckSum:      checksum,
        }

        WALRecords = append(WALRecords, WALRecord)
    }

    if len(WALRecords) == 0 {
        return 0, WALInput{}
    }

    return len(WALRecords), WALRecords[len(WALRecords)-1]
}

