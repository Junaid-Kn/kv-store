package main

import (
	"bufio"
	"log"
	"net"
	"os"
	"strings"
	"path/filepath"
	"github.com/Junaid-Kn/kv-store/storage_engine"
)

const PORT = 9000

func main() {
	// Make sure a data directory was provided.
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <data-directory>", os.Args[0])
	}

	dataDir, err := filepath.Abs(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	// Create a KVStorage using the directory provided by the user.
	s, err := storage_engine.NewKVStorage(dataDir)
	if err != nil{
		log.Fatal(err)
	}
	value, err := s.Read([]byte("key-00001"))
	if err != nil {
		log.Printf("STARTUP READ ERROR: %v", err)
	} else {
		log.Printf("STARTUP READ: %s", value)
	}

	// Start Listener
	l, err := net.Listen("tcp", "0.0.0.0:9000")
	if err != nil {
		log.Printf("Could not start TCP listener: %s", err)
		return
	}
	defer l.Close()

	log.Printf("Listening on port %d", PORT)
	log.Printf("Using data directory: %s", dataDir)

	// Wait for new connections
	for {
		// Accept new connections
		c, err := l.Accept()
		if err != nil {
			log.Printf("Listener returned: %s", err)
			break
		}

		// Kickoff a Goroutine to handle the new connection
		go func() {
			defer c.Close()

			log.Printf("New connection created")

			scanner := bufio.NewScanner(c)

			for scanner.Scan() {
				input := scanner.Text()

				parts := strings.Fields(input)
				if len(parts) == 0 {
					continue
				}

				// Currently hardcoded for 2, later will implement
				// for multiple commands at once.
				if len(parts) != 2 {
					log.Printf("Command not registered: %s", input)
					continue
				}

				operation := parts[0]

				switch operation {
				case "GET":
					key := []byte(parts[1])

					value, err := s.Read(key)
					if err != nil {
						c.Write([]byte("Error: " + err.Error() + "\n"))
						continue
					}

   					c.Write([]byte(value + "\n"))
				}

				log.Printf("Received: %s", input)

			}

			if err := scanner.Err(); err != nil {
				log.Printf("Connection error: %s", err)
			}

			log.Printf("Connection closed")
		}()
	}
}

