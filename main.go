package main

import (
	"fmt"
	"log"
	"os"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func main() {
	// Defina o ano que você deseja filtrar
	anoFiltro := "2022"

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

	// Filtrar o DataFrame pelo ano especificado
	dfFiltrado := df.Filter(
		dataframe.F{
			Colname:    "ano_acidente",
			Comparator: series.Eq,
			Comparando: anoFiltro,
		},
	)

	// Verificar se a filtragem foi realizada corretamente
	if dfFiltrado.Err != nil {
		log.Fatal(dfFiltrado.Err)
	}

	// Agrupar por UF e contar o número de ocorrências
	grouped := dfFiltrado.GroupBy("uf_acidente")
	counts := grouped.Aggregation([]dataframe.AggregationType{dataframe.Aggregation_COUNT}, []string{"uf_acidente"})

	// Renomear a coluna de contagem para "num_acidentes" para maior clareza
	counts = counts.Rename("num_acidentes", "uf_acidente_COUNT")

	// Ordenar os estados pela contagem de acidentes em ordem decrescente
	sorted := counts.Arrange(dataframe.RevSort("num_acidentes"))

	// Verificar se a ordenação foi realizada corretamente
	if sorted.Err != nil {
		log.Fatal(sorted.Err)
	}

	// Itera sobre cada linha do DataFrame e a imprime
	for i, row := range sorted.Records() {
		// Imprime o índice da linha e os dados da linha
		fmt.Printf("Linha %d: %v\n", i, row)
	}

	// Adicionalmente, imprimir o DataFrame completo para verificar os dados
	fmt.Println("DataFrame completo agrupado e ordenado:")
	fmt.Println(sorted)
}
