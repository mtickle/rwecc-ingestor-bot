package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// Incident struct matches the JSON object structure from the API.
type Incident struct {
	Jurisdiction string  `json:"jurisdiction"`
	Problem      string  `json:"problem"`
	Address      string  `json:"address"`
	Lat          float64 `json:"lat"`
	Long         float64 `json:"long"`
	Timestamp    string  `json:"timestamp"`
}

// saveToUnifiedDB is the new core function. It normalizes and saves an incident.
func saveToUnifiedDB(db *sql.DB, incident Incident) error {
	source := "RWECC"
	sourceID := incident.Timestamp + " " + incident.Address
	eventType := "Vehicle Crash"

	// --- THE FIX ---
	// Load the Eastern Time location.
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		return fmt.Errorf("could not load time zone location: %w", err)
	}

	// Parse the incoming timestamp string AS Eastern Time.
	parsedTime, err := time.ParseInLocation("2006-01-02 15:04:05.000", incident.Timestamp, loc)
	if err != nil {
		log.Printf("Could not parse timestamp '%s', using current time. Error: %v", incident.Timestamp, err)
		parsedTime = time.Now()
	}
	// The database driver will automatically convert this to UTC for storage.

	detailsJSON, err := json.Marshal(incident)
	if err != nil {
		return fmt.Errorf("could not marshal incident details to JSON: %w", err)
	}

	sqlStatement := `
		INSERT INTO unified_incidents (
			source, source_id, event_type, status, address, latitude, longitude, timestamp, details
		) VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, $8)
		ON CONFLICT (source, source_id) DO NOTHING;
	`

	_, err = db.Exec(sqlStatement,
		source, sourceID, eventType, incident.Address, incident.Lat, incident.Long, parsedTime, detailsJSON,
	)
	return err
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Note: .env file not found, reading credentials from environment")
	}

	// --- Database Connection ---
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
		os.Getenv("DATABASE_HOST"), os.Getenv("DATABASE_PORT"), os.Getenv("DATABASE_USERNAME"),
		os.Getenv("DATABASE_PASSWORD"), os.Getenv("DATABASE_NAME"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Error opening database: %s", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Error connecting to database: %s", err)
	}
	log.Println("Successfully connected to the database.")

	// --- Fetch and Process Data ---
	apiURL := os.Getenv("RWECC_URL")
	if apiURL == "" {
		log.Fatalln("Error: RWECC_URL must be set in your environment or .env file.")
	}

	resp, err := http.Get(apiURL)
	if err != nil {
		log.Fatalf("Error fetching data from API: %s", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading API response body: %s", err)
	}

	var incidents []Incident
	if err := json.Unmarshal(body, &incidents); err != nil {
		log.Fatalf("Error unmarshalling JSON: %s", err)
	}

	log.Println("Searching for new MVC Incidents from RWECC API...")
	incidentsSaved := 0

	for _, incident := range incidents {
		// We only care about MVC incidents for this feed
		if strings.Contains(incident.Problem, "MVC") {
			if err := saveToUnifiedDB(db, incident); err != nil {
				log.Printf("Error saving incident for '%s': %v", incident.Address, err)
			} else {
				incidentsSaved++
			}
		}
	}

	log.Printf("Run complete. Processed and saved %d MVC incidents to the unified table.", incidentsSaved)
}

