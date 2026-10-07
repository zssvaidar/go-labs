package main

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// newServer builds the Echo instance with middleware and routes.
func newServer() *echo.Echo {
	e := echo.New()
	e.HideBanner = true

	e.Use(middleware.RequestLogger())
	e.Use(middleware.Recover())

	e.GET("/health", health)
	e.GET("/hello", hello)

	return e
}

func health(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

// hello greets ?name=, defaulting to "world".
func hello(c echo.Context) error {
	name := c.QueryParam("name")
	if name == "" {
		name = "world"
	}
	return c.JSON(http.StatusOK, map[string]string{"message": "Hello, " + name + "!"})
}
