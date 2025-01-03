package main

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

func configureRouter() *fiber.App {
	router := fiber.New()
	router.Use(logger.New())

	router.Get("/ping", func(c *fiber.Ctx) error {
		return c.SendString("Pong!")
	})

	return router
}
