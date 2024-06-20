package estados

import (
	"log"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func Estados(ano string, df dataframe.DataFrame) dataframe.DataFrame {

	// Filtrar o DataFrame pelo ano especificado
	dfFiltrado := df.Filter(
		dataframe.F{
			Colname:    "ano_acidente",
			Comparator: series.Eq,
			Comparando: ano,
		},
	)

	// Commita

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

	return sorted
}
