package basic

import (
	"github.com/sethvargo/go-password/password"
)

type PasswordGenerator struct {
	generator *password.Generator
}

func NewPasswordGenerator() PasswordGenerator {
	generator, err := password.NewGenerator(&password.GeneratorInput{Symbols: "_#$@"})
	if err != nil {
		panic(err)
	}
	return PasswordGenerator{generator: generator}
}

func (operatorGenerator PasswordGenerator) Generate() (string, error) {
	return operatorGenerator.generator.Generate(10, 1, 1, false, false)
}
