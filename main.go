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

// --- Structs for the National Weather Service (NWS) API ---
type NWSPointsResponse struct {
	Properties struct {
		ForecastHourly string `json:"forecastHourly"`
	} `json:"properties"`
}

type NWSHourlyResponse struct {
	Properties struct {
		Periods []WeatherData `json:"periods"`
	} `json:"properties"`
}

// WeatherData holds the current weather conditions from the NWS.
type WeatherData struct {
	Temperature   int    `json:"temperature"`
	WindSpeed     string `json:"windSpeed"`
	ShortForecast string `json:"shortForecast"`
	Icon          string `json:"icon"`
}

// getWeatherForIncident fetches current weather conditions from the NWS API.
func getWeatherForIncident(lat, lon float64) (*WeatherData, error) {
	pointsURL := fmt.Sprintf("https://api.weather.gov/points/%.4f,%.4f", lat, lon)
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", pointsURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "(patrolx, mtickle@gmail.com)")

	pointsResp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch NWS points data: %w", err)
	}
	defer pointsResp.Body.Close()
	if pointsResp.StatusCode != 200 {
		return nil, fmt.Errorf("NWS points API returned non-200 status: %s", pointsResp.Status)
	}
	body, err := io.ReadAll(pointsResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read NWS points response body: %w", err)
	}
	var pointsResponse NWSPointsResponse
	if err := json.Unmarshal(body, &pointsResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal NWS points JSON: %w", err)
	}
	if pointsResponse.Properties.ForecastHourly == "" {
		return nil, fmt.Errorf("NWS points response did not contain a forecast URL")
	}

	req, err = http.NewRequest("GET", pointsResponse.Properties.ForecastHourly+"?units=us", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "(patrolx, mtickle@gmail.com)")
	hourlyResp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch NWS hourly data: %w", err)
	}
	defer hourlyResp.Body.Close()
	if hourlyResp.StatusCode != 200 {
		return nil, fmt.Errorf("NWS hourly API returned non-200 status: %s", hourlyResp.Status)
	}
	hourlyBody, err := io.ReadAll(hourlyResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read NWS hourly response body: %w", err)
	}
	var hourlyResponse NWSHourlyResponse
	if err := json.Unmarshal(hourlyBody, &hourlyResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal NWS hourly JSON: %w", err)
	}
	if len(hourlyResponse.Properties.Periods) > 0 {
		return &hourlyResponse.Properties.Periods[0], nil
	}
	return nil, fmt.Errorf("no weather periods returned from NWS")
}

// saveToUnifiedDB normalizes and saves an incident to the unified table.
func saveToUnifiedDB(db *sql.DB, incident Incident) error {
	source := "RWECC"
	sourceID := incident.Timestamp + " " + incident.Address
	eventType := "Vehicle Crash"

	loc, _ := time.LoadLocation("America/New_York")
	parsedTime, err := time.ParseInLocation("2006-01-02 15:04:05.000", incident.Timestamp, loc)
	if err != nil {
		log.Printf("Could not parse timestamp '%s', using current time. Error: %v", incident.Timestamp, err)
		parsedTime = time.Now()
	}

	// --- ENRICHMENT STEP ---
	weatherData, err := getWeatherForIncident(incident.Lat, incident.Long)
	if err != nil {
		log.Printf("Warning: could not fetch weather for incident '%s': %v", incident.Address, err)
	}

	details := map[string]interface{}{
		"raw_incident": incident,
		"weather":      weatherData,
	}

	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("could not marshal unified details to JSON: %w", err)
	}

	// --- PREPARE NEW COLUMN VALUES ---
	var weatherTemp sql.NullInt32
	var weatherWind, weatherForecast sql.NullString

	if weatherData != nil {
		weatherTemp.Int32 = int32(weatherData.Temperature)
		weatherTemp.Valid = true
		weatherWind.String = weatherData.WindSpeed
		weatherWind.Valid = true
		weatherForecast.String = weatherData.ShortForecast
		weatherForecast.Valid = true
	}

	// Updated SQL to populate jurisdiction, problem_detail, and weather columns
	sqlStatement := `
		INSERT INTO unified_incidents (
			source, source_id, event_type, status, address, latitude, longitude, timestamp, details,
			jurisdiction, problem_detail, weather_temp, weather_wind_speed, weather_forecast
		) VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (source, source_id) DO UPDATE SET
			details = EXCLUDED.details,
			status = 'active',
			jurisdiction = EXCLUDED.jurisdiction,
			problem_detail = EXCLUDED.problem_detail,
			weather_temp = EXCLUDED.weather_temp,
			weather_wind_speed = EXCLUDED.weather_wind_speed,
			weather_forecast = EXCLUDED.weather_forecast;
	`

	_, err = db.Exec(sqlStatement,
		source, sourceID, eventType, incident.Address, incident.Lat, incident.Long, parsedTime, detailsJSON,
		incident.Jurisdiction, incident.Problem, weatherTemp, weatherWind, weatherForecast,
	)
	return err
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Note: .env file not found")
	}

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

	apiURL := os.Getenv("RWECC_URL")
	if apiURL == "" {
		log.Fatalln("Error: RWECC_URL must be set.")
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
