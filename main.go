package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"

	"github.com/go-gota/gota/dataframe"
	"github.com/gota-tutorial/estados"
)

func main() {
	// Defina o ano que você deseja filtrar
	var anoFiltro string

	// Abrir o arquivo CSV
	file, err := os.Open("Acidentes_DadosAbertos_20230412.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Ler o CSV em um DataFrame com o separador correto
	df := dataframe.ReadCSV(file, dataframe.WithDelimiter(';'))

	// Verificar se o DataFrame foi carregado corretamente
	if df.Err != nil {
		log.Fatal(df.Err)
	}

	fmt.Println("Digite um ano por exemplo 2021")
	fmt.Scan(&anoFiltro)
	sorted := estados.Estados(anoFiltro, df)

	// Itera sobre cada linha do DataFrame e a imprime
	for i, row := range sorted.Records() {
		// Imprime o índice da linha e os dados da linha
		if i == 0 {
			fmt.Printf("Linha %d: %s\n", i, row)
		} else {
			num, _ := strconv.ParseFloat(row[1], 64)
			// fmt.Println(math.Round(num))
			fmt.Printf("Linha %d: %v %v\n", i, row[0], math.Round(num))
			// fmt.Println(i, row[0], math.Round(num))
		}
	}
}
