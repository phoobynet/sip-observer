package config

import (
	"errors"
	"fmt"
	"github.com/pelletier/go-toml/v2"
	"github.com/phoobynet/sip-observer/file"
	"github.com/phoobynet/sip-observer/utils"
	"os"
)

type Config struct {
	Title   string
	Symbols []string
	DBHost  string `toml:"db_host"`
}

func Load(configPath string) (*Config, error) {
	file.MustExist(configPath)

	var config *Config

	data, err := os.ReadFile(configPath)

	if err != nil {
		return nil, err
	}

	err = toml.Unmarshal(data, &config)

	if err != nil {
		return nil, err
	}

	config.clean()

	if err := config.validate(); err != nil {
		return nil, err
	}

	return config, nil
}

func (c *Config) String() string {
	return fmt.Sprintf("title: %s, symbols: %v, db_host: %s", c.Title, c.Symbols, c.DBHost)
}

// clean removes any invalid characters from the ticker symbols, trims whitespace and converts to uppercase.
func (c *Config) clean() {
	var cleanedSymbols []string
	var cleanedSymbol string

	for _, symbol := range c.Symbols {
		cleanedSymbol = utils.CleanTicker(symbol)
		if cleanedSymbol != "" {
			cleanedSymbols = append(cleanedSymbols, cleanedSymbol)
		}
	}

	c.Symbols = cleanedSymbols
}

func (c *Config) validate() error {
	if c.Title == "" {
		return errors.New("title is required")
	}

	if len(c.Symbols) == 0 {
		return errors.New("symbols is required")
	}

	if c.DBHost == "" {
		return errors.New("db_host is required")
	}

	return nil
}
