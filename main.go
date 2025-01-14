package main

import (
	"database/sql"
	"errors"
	"log"
	"math/rand"
	"os"

	"github.com/gofiber/fiber/v2"
	_ "github.com/lib/pq"
)

var db *sql.DB

func SetupSchema() {
	schemaQuery := `
    CREATE TABLE IF NOT EXISTS short_urls (
        id SERIAL PRIMARY KEY,
        key VARCHAR(20) NOT NULL UNIQUE,
        original_url TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `
	if _, err := db.Exec(schemaQuery); err != nil {
		log.Fatalf("[DATABASE]: unable to setup schema:\n%v", err)
	}
}

const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const idLength = 6

var totalCombinations = int64(len(charset)) ^ idLength

func getShortURLByOriginalURL(db *sql.DB, originalURL string) (string, error) {
	var shortURL string
	query := "SELECT key FROM short_urls WHERE original_url = $1 LIMIT 1"
	if err := db.QueryRow(query, originalURL).Scan(&shortURL); err != nil {
		return "", err
	}
	return shortURL, nil
}

func createShortURL(db *sql.DB, originalURL string) (string, error) {
	shortKey, err := RandomShortID(map[string]bool{})
	if err != nil {
		return "", err
	}
	query := "INSERT INTO short_urls (key, original_url) VALUES ($1, $2) RETURNING key"
	if err := db.QueryRow(query, shortKey, originalURL).Scan(&shortKey); err != nil {
		return "", err
	}
	return shortKey, nil
}

func RandomShortID(existingIDs map[string]bool) (string, error) {
	if int64(len(existingIDs)) >= totalCombinations {
		return "", errors.New("no more unique IDs can be generated")
	}
	id := generateRandomID()
	for existingIDs[id] {
		id = generateRandomID()
	}
	existingIDs[id] = true
	return id, nil
}

func generateRandomID() string {
	id := make([]byte, idLength)
	for i := range id {
		id[i] = charset[rand.Intn(len(charset))]
	}
	return string(id)
}

func GetOrCreateShortURL(ctx *fiber.Ctx) error {
	var requestBody struct {
		OriginalURL string `json:"original_url"`
	}

	if err := ctx.BodyParser(&requestBody); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid input",
		})
	}

	originalURL := requestBody.OriginalURL

	shortURL, err := getShortURLByOriginalURL(db, originalURL)

	if err == sql.ErrNoRows {
		shortKey, err := createShortURL(db, originalURL)

		if err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to create short URL",
			})
		}

		return ctx.JSON(fiber.Map{
			"original_url": originalURL,
			"short_url":    shortKey,
		})
	} else if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to check for existing short URL",
		})
	}

	return ctx.JSON(fiber.Map{
		"original_url": originalURL,
		"short_url":    shortURL,
	})
}

func RedirectToCorrespondingURL(ctx *fiber.Ctx) error {
	shortKey := ctx.Params("key")

	var originalURL string
	query := "SELECT original_url FROM short_urls WHERE key = $1 LIMIT 1"
	err := db.QueryRow(query, shortKey).Scan(&originalURL)

	if err != nil {
		if err == sql.ErrNoRows {
			return ctx.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Short URL not found",
			})
		}
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to retrieve original URL",
		})
	}

	return ctx.Redirect(originalURL, fiber.StatusMovedPermanently)
}

func main() {
	connectionString := os.Getenv("DATABASE_URL")
	var err error
	db, err = sql.Open("postgres", connectionString)

	if err != nil {
		log.Fatalf("[DATABASE]: can't connect to database:\n%v", err)
	}

	defer db.Close()

	SetupSchema()

	app := fiber.New()

	app.Get("/:key", RedirectToCorrespondingURL)
	app.Post("/", GetOrCreateShortURL)

	app.Listen(":3000")
}
